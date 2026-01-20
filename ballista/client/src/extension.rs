// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

pub use ballista_core::extension::{SessionConfigExt, SessionStateExt};
use ballista_core::serde::protobuf::execute_query_params::Query::SubstraitPlan;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{ExecuteQueryParams, execute_query_result};
use ballista_core::utils::{GrpcClientConfig, create_grpc_client_connection};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::{
    error::DataFusionError, execution::SessionState, prelude::SessionContext,
};
use url::Url;

const DEFAULT_SCHEDULER_PORT: u16 = 50050;

/// Module provides [SessionContextExt] which adds `standalone*` and `remote*`
/// methods to [SessionContext].
///
/// Provided methods set up [SessionContext] with [BallistaQueryPlanner](ballista_core::utils), which
/// handles running plans on Ballista clusters.
///
///```no_run
/// use ballista::prelude::SessionContextExt;
/// use datafusion::prelude::SessionContext;
///
/// # #[tokio::main]
/// # async fn main() -> datafusion::error::Result<()> {
/// let ctx: SessionContext = SessionContext::remote("df://localhost:50050").await?;
/// # Ok(())
/// # }
///```
///
/// [SessionContextExt::standalone()] provides an easy way to start up
/// local cluster. It is an optional feature which should be enabled
/// with `standalone`
///
///```no_run
/// use ballista::prelude::SessionContextExt;
/// use datafusion::prelude::SessionContext;
///
/// # #[tokio::main]
/// # async fn main() -> datafusion::error::Result<()> {
/// let ctx: SessionContext = SessionContext::standalone().await?;
/// # Ok(())
/// # }
///```
///
/// There are still few limitations on query distribution, thus not all
/// [SessionContext] functionalities are supported.
///

#[async_trait::async_trait]
pub trait SessionContextExt {
    /// Creates a context for executing queries against a standalone Ballista scheduler instance
    ///
    /// It wills start local ballista cluster with scheduler and executor.
    #[cfg(feature = "standalone")]
    async fn standalone() -> datafusion::error::Result<SessionContext>;

    /// Creates a context for executing queries against a standalone Ballista scheduler instance
    /// with custom session state.
    ///
    /// It wills start local ballista cluster with scheduler and executor.
    #[cfg(feature = "standalone")]
    async fn standalone_with_state(
        state: SessionState,
    ) -> datafusion::error::Result<SessionContext>;

    /// Creates a context for executing queries against a remote Ballista scheduler instance
    async fn remote(url: &str) -> datafusion::error::Result<SessionContext>;

    /// Creates a context for executing queries against a remote Ballista scheduler instance
    /// with custom session state
    async fn remote_with_state(
        url: &str,
        state: SessionState,
    ) -> datafusion::error::Result<SessionContext>;
}

#[async_trait::async_trait]
impl SessionContextExt for SessionContext {
    async fn remote_with_state(
        url: &str,
        state: SessionState,
    ) -> datafusion::error::Result<SessionContext> {
        let scheduler_url = Extension::parse_url(url)?;
        log::info!(
            "Connecting to Ballista scheduler at {}",
            scheduler_url.clone()
        );

        let session_state = state.upgrade_for_ballista(scheduler_url)?;

        log::info!(
            "Server side SessionContext created with session id: {}",
            session_state.session_id()
        );

        Ok(SessionContext::new_with_state(session_state))
    }

    async fn remote(url: &str) -> datafusion::error::Result<SessionContext> {
        let scheduler_url = Extension::parse_url(url)?;
        log::info!(
            "Connecting to Ballista scheduler at: {}",
            scheduler_url.clone()
        );

        let session_state = SessionState::new_ballista_state(scheduler_url)?;
        log::info!(
            "Server side SessionContext created with session id: {}",
            session_state.session_id()
        );

        Ok(SessionContext::new_with_state(session_state))
    }

    #[cfg(feature = "standalone")]
    async fn standalone_with_state(
        state: SessionState,
    ) -> datafusion::error::Result<SessionContext> {
        let scheduler_url = Extension::setup_standalone(Some(&state)).await?;

        let session_state = state.upgrade_for_ballista(scheduler_url)?;

        log::info!(
            "Server side SessionContext created with session id: {}",
            session_state.session_id()
        );

        Ok(SessionContext::new_with_state(session_state))
    }

    #[cfg(feature = "standalone")]
    async fn standalone() -> datafusion::error::Result<Self> {
        log::info!("Running in local mode. Scheduler will be run in-proc");

        let scheduler_url = Extension::setup_standalone(None).await?;

        let session_state = SessionState::new_ballista_state(scheduler_url)?;

        log::info!(
            "Server side SessionContext created with session id: {}",
            session_state.session_id()
        );

        Ok(SessionContext::new_with_state(session_state))
    }
}

#[cfg(feature = "standalone")]
/// Extension can be used to set up a standalone in-proc scheduler instance
/// without attaching it to SessionContext. This can be useful for testing
/// Ballista with a custom front-end.
///
/// ```
/// use ballista::extension::Extension;
///
/// # #[tokio::main]
/// # async fn main() -> datafusion::error::Result<()> {
/// let scheduler_url = Extension::setup_standalone(None);
/// # Ok(())
/// # }
/// ```
pub struct Extension {}

#[cfg(not(feature = "standalone"))]
struct Extension {}

impl Extension {
    fn parse_url(url: &str) -> datafusion::error::Result<String> {
        let url =
            Url::parse(url).map_err(|e| DataFusionError::Configuration(e.to_string()))?;
        let host = url.host().ok_or(DataFusionError::Configuration(
            "hostname should be provided".to_string(),
        ))?;
        let port = url.port().unwrap_or(DEFAULT_SCHEDULER_PORT);
        let scheduler_url = format!("http://{}:{}", &host, port);

        Ok(scheduler_url)
    }

    #[cfg(feature = "standalone")]
    /// Creates in-process scheduler and executor.
    pub async fn setup_standalone(
        session_state: Option<&SessionState>,
    ) -> datafusion::error::Result<String> {
        use ballista_core::{serde::BallistaCodec, utils::default_config_producer};

        let addr = match session_state {
            None => ballista_scheduler::standalone::new_standalone_scheduler()
                .await
                .map_err(|e| DataFusionError::Configuration(e.to_string()))?,
            Some(session_state) => {
                ballista_scheduler::standalone::new_standalone_scheduler_from_state(
                    session_state,
                )
                .await
                .map_err(|e| DataFusionError::Configuration(e.to_string()))?
            }
        };
        let config = session_state
            .map(|s| s.config().clone())
            .unwrap_or_else(default_config_producer);

        let scheduler_url = format!("http://localhost:{}", addr.port());

        let scheduler = loop {
            match SchedulerGrpcClient::connect(scheduler_url.clone()).await {
                Err(_) => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    log::info!("Attempting to connect to in-proc scheduler...");
                }
                Ok(scheduler) => break scheduler,
            }
        };

        let concurrent_tasks = config.ballista_standalone_parallelism();

        match session_state {
            None => {
                ballista_executor::new_standalone_executor(
                    scheduler,
                    concurrent_tasks,
                    BallistaCodec::default(),
                )
                .await
                .map_err(|e| DataFusionError::Configuration(e.to_string()))?;
            }
            Some(session_state) => {
                ballista_executor::new_standalone_executor_from_state(
                    scheduler,
                    concurrent_tasks,
                    session_state,
                )
                .await
                .map_err(|e| DataFusionError::Configuration(e.to_string()))?;
            }
        }

        Ok(scheduler_url)
    }
}

#[cfg(feature = "substrait")]
/// Wrapper class to simplify execution of Substrait plans.
pub struct SubstraitSchedulerClient {
    scheduler_url: String,
    session_id: String,
    grpc_config: GrpcClientConfig,
    max_message_size: usize,
    use_flight_transport: bool,
}

#[cfg(not(feature = "substrait"))]
struct SubstraitExec {}

#[cfg(feature = "substrait")]
impl SubstraitSchedulerClient {
    /// Creates a new SubstraitExec with default configuration.
    ///
    /// # Defaults
    /// - gRPC timeout: 20 seconds
    /// - Max message size: 16MB
    /// - Transport: Block-based (faster than Flight for large results)
    ///
    /// # Example
    /// ```no_run
    /// use ballista::extension::SubstraitSchedulerClient;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> datafusion::error::Result<()> {
    /// let client = SubstraitSchedulerClient::new(
    ///     "http://localhost:50050".to_string(),
    ///     "session-123".to_string()
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        scheduler_url: String,
        session_id: String,
    ) -> datafusion::error::Result<Self> {
        Ok(Self {
            scheduler_url,
            session_id,
            grpc_config: GrpcClientConfig::default(),
            max_message_size: 16 * 1024 * 1024, // 16MB
            use_flight_transport: false,
        })
    }

    /// Creates a builder for custom configuration.
    ///
    /// # Example
    /// ```no_run
    /// use ballista::extension::SubstraitSchedulerClient;
    /// use ballista_core::utils::GrpcClientConfig;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> datafusion::error::Result<()> {
    /// let custom_config = GrpcClientConfig {
    ///     connect_timeout_seconds: 60,
    ///     timeout_seconds: 120,
    ///     ..Default::default()
    /// };
    ///
    /// let client = SubstraitSchedulerClient::builder(
    ///     "http://localhost:50050".to_string(),
    ///     "session-123".to_string()
    /// )
    /// .with_grpc_config(custom_config)
    /// .with_max_message_size(32 * 1024 * 1024)
    /// .build()
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(scheduler_url: String, session_id: String) -> SubstraitExecBuilder {
        SubstraitExecBuilder::new(scheduler_url, session_id)
    }

    /// Executes a Substrait query plan and returns a stream of results.
    ///
    /// This method:
    /// 1. Submits the Substrait plan to the scheduler
    /// 2. Polls until the job completes
    /// 3. Fetches result partitions from executors
    /// 4. Returns a lazy stream of RecordBatches
    ///
    /// # Arguments
    /// * `plan` - Serialized Substrait plan bytes
    ///
    /// # Returns
    /// A `SendableRecordBatchStream` that lazily fetches and yields result batches.
    ///
    /// # Example
    /// ```no_run
    /// use ballista::extension::SubstraitSchedulerClient;
    /// use futures::StreamExt;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> datafusion::error::Result<()> {
    /// let client = SubstraitSchedulerClient::new(
    ///     "http://localhost:50050".to_string(),
    ///     "session-123".to_string()
    /// ).await?;
    ///
    /// let substrait_bytes = vec![]; // Your Substrait plan
    /// let mut stream = client.execute_query(substrait_bytes).await?;
    ///
    /// while let Some(batch) = stream.next().await {
    ///     let batch = batch?;
    ///     println!("Got {} rows", batch.num_rows());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_query(
        &self,
        plan: Vec<u8>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        use ballista_core::execution_plans::{fetch_partitions, poll_job};

        let query_start_time = std::time::Instant::now();

        // Connect to scheduler
        let connection =
            create_grpc_client_connection(self.scheduler_url.clone(), &self.grpc_config)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to connect to scheduler: {e:?}"
                    ))
                })?;

        let mut scheduler = SchedulerGrpcClient::new(connection)
            .max_encoding_message_size(self.max_message_size)
            .max_decoding_message_size(self.max_message_size);

        // Submit Substrait plan
        let execute_query_params = ExecuteQueryParams {
            session_id: self.session_id.clone(),
            settings: vec![],
            operation_id: uuid::Uuid::now_v7().to_string(),
            query: Some(SubstraitPlan(plan)),
        };

        let response = scheduler
            .execute_query(execute_query_params)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to execute query: {e:?}"))
            })?
            .into_inner();

        let query_result = match response.result {
            Some(execute_query_result::Result::Success(success)) => success,
            Some(execute_query_result::Result::Failure(failure)) => {
                return Err(DataFusionError::Execution(format!(
                    "Query execution failed: {:?}",
                    failure
                )));
            }
            None => {
                return Err(DataFusionError::Execution(
                    "No result received from scheduler".to_string(),
                ));
            }
        };

        // Verify session consistency
        if query_result.session_id != self.session_id {
            return Err(DataFusionError::Execution(format!(
                "Session ID mismatch: expected {}, got {}",
                self.session_id, query_result.session_id
            )));
        }

        // Poll for job completion
        let successful_job = poll_job(
            &mut scheduler,
            query_result.job_id,
            None, // No metrics for SubstraitExec (not an ExecutionPlan)
            0,    // partition number (not used without metrics)
            query_start_time,
        )
        .await?;

        // Fetch partitions from executors
        let partition_stream = fetch_partitions(
            successful_job.partition_location,
            self.max_message_size,
            self.use_flight_transport,
        )
        .await?;

        // Wrap in SubstraitResultStream to implement RecordBatchStream
        Ok(Box::pin(SubstraitResultStream::new(partition_stream)))
    }
}

#[cfg(feature = "substrait")]
/// Builder for [SubstraitSchedulerClient]
pub struct SubstraitExecBuilder {
    scheduler_url: String,
    session_id: String,
    grpc_config: Option<GrpcClientConfig>,
    max_message_size: Option<usize>,
    use_flight_transport: Option<bool>,
}

#[cfg(feature = "substrait")]
impl SubstraitExecBuilder {
    fn new(scheduler_url: String, session_id: String) -> Self {
        Self {
            scheduler_url,
            session_id,
            grpc_config: None,
            max_message_size: None,
            use_flight_transport: None,
        }
    }

    /// Overrides default gRPC configuration
    pub fn with_grpc_config(mut self, config: GrpcClientConfig) -> Self {
        self.grpc_config = Some(config);
        self
    }

    /// Sets max message size for gRPC client
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Sets usage of Flight when communicating with executor
    pub fn with_flight_transport(mut self, use_flight: bool) -> Self {
        self.use_flight_transport = Some(use_flight);
        self
    }

    /// Creates a [SubstraitSchedulerClient] instance
    pub async fn build(self) -> datafusion::error::Result<SubstraitSchedulerClient> {
        Ok(SubstraitSchedulerClient {
            scheduler_url: self.scheduler_url,
            session_id: self.session_id,
            grpc_config: self.grpc_config.unwrap_or_default(),
            max_message_size: self.max_message_size.unwrap_or(16 * 1024 * 1024),
            use_flight_transport: self.use_flight_transport.unwrap_or(false),
        })
    }
}

#[cfg(feature = "substrait")]
struct SubstraitResultStream<S> {
    inner: S,
    schema: Option<datafusion::arrow::datatypes::SchemaRef>,
}

#[cfg(feature = "substrait")]
impl<S> SubstraitResultStream<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            schema: None,
        }
    }
}

#[cfg(feature = "substrait")]
impl<S> futures::Stream for SubstraitResultStream<S>
where
    S: futures::Stream<
            Item = datafusion::error::Result<
                datafusion::arrow::record_batch::RecordBatch,
            >,
        >,
{
    type Item = datafusion::error::Result<datafusion::arrow::record_batch::RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // SAFETY: We're only accessing inner which is a stream, and schema which is Option<Arc>
        // We never move out of self
        let this = unsafe { self.get_unchecked_mut() };
        // SAFETY: inner is never moved, only accessed through pinned reference
        let inner_pin = unsafe { std::pin::Pin::new_unchecked(&mut this.inner) };

        match inner_pin.poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(batch))) => {
                // Capture schema from first batch
                if this.schema.is_none() {
                    this.schema = Some(batch.schema());
                }
                std::task::Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}

#[cfg(feature = "substrait")]
impl<S> datafusion::physical_plan::RecordBatchStream for SubstraitResultStream<S>
where
    S: futures::Stream<
            Item = datafusion::error::Result<
                datafusion::arrow::record_batch::RecordBatch,
            >,
        > + Send,
{
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone().unwrap_or_else(|| {
            std::sync::Arc::new(datafusion::arrow::datatypes::Schema::empty())
        })
    }
}
