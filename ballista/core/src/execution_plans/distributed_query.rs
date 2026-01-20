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

use crate::config::BallistaConfig;
use crate::execution_plans::query_executor;
use crate::serde::protobuf::{
    ExecuteQueryParams, KeyValuePair, execute_query_params::Query, execute_query_result,
    scheduler_grpc_client::SchedulerGrpcClient,
};
use crate::utils::{GrpcClientConfig, create_grpc_client_connection};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion_proto::logical_plan::{
    AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use log::{debug, info};
use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

/// This operator sends a logical plan to a Ballista scheduler for execution and
/// polls the scheduler until the query is complete and then fetches the resulting
/// batches directly from the executors that hold the results from the final
/// query stage.
#[derive(Debug, Clone)]
pub struct DistributedQueryExec<T: 'static + AsLogicalPlan> {
    /// Ballista scheduler URL
    scheduler_url: String,
    /// Ballista configuration
    config: BallistaConfig,
    /// Logical plan to execute
    plan: LogicalPlan,
    /// Codec for LogicalPlan extensions
    extension_codec: Arc<dyn LogicalExtensionCodec>,
    /// Phantom data for serializable plan message
    plan_repr: PhantomData<T>,
    /// Session id
    session_id: String,
    /// Plan properties
    properties: PlanProperties,
    /// Execution metrics, currently exposes:
    /// - output_rows: Total number of rows returned
    /// - transferred_bytes: Total bytes transferred from executors
    /// - job_execution_time_ms: Time spent executing on the cluster (server-side)
    /// - job_scheduling_in_ms: Time from query submission to job start (includes queue time)
    /// - job_execution_time_ms: Time spent executing on the cluster (ended_at - started_at)
    /// - job_scheduling_in_ms: Time job waited in scheduler queue (started_at - queued_at)
    metrics: ExecutionPlanMetricsSet,
}

impl<T: 'static + AsLogicalPlan> DistributedQueryExec<T> {
    /// Creates a new distributed query execution plan.
    pub fn new(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        session_id: String,
    ) -> Self {
        let properties =
            Self::compute_properties(plan.schema().as_arrow().clone().into());
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            plan_repr: PhantomData,
            session_id,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Creates a new distributed query execution plan with a custom extension codec.
    pub fn with_extension(
        scheduler_url: String,
        config: BallistaConfig,
        plan: LogicalPlan,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        session_id: String,
    ) -> Self {
        let properties =
            Self::compute_properties(plan.schema().as_arrow().clone().into());
        Self {
            scheduler_url,
            config,
            plan,
            extension_codec,
            plan_repr: PhantomData,
            session_id,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl<T: 'static + AsLogicalPlan> DisplayAs for DistributedQueryExec<T> {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DistributedQueryExec: scheduler_url={}",
                    self.scheduler_url
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "scheduler_url={}", self.scheduler_url)
            }
        }
    }
}

impl<T: 'static + AsLogicalPlan> ExecutionPlan for DistributedQueryExec<T> {
    fn name(&self) -> &str {
        "DistributedQueryExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.plan.schema().as_arrow().clone().into()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DistributedQueryExec {
            scheduler_url: self.scheduler_url.clone(),
            config: self.config.clone(),
            plan: self.plan.clone(),
            extension_codec: self.extension_codec.clone(),
            plan_repr: self.plan_repr,
            session_id: self.session_id.clone(),
            properties: Self::compute_properties(
                self.plan.schema().as_arrow().clone().into(),
            ),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(0, partition);

        let mut buf: Vec<u8> = vec![];
        let plan_message = T::try_from_logical_plan(
            &self.plan,
            self.extension_codec.as_ref(),
        )
        .map_err(|e| {
            DataFusionError::Internal(format!("failed to serialize logical plan: {e:?}"))
        })?;
        plan_message.try_encode(&mut buf).map_err(|e| {
            DataFusionError::Execution(format!("failed to encode logical plan: {e:?}"))
        })?;

        let settings = context
            .session_config()
            .options()
            .entries()
            .iter()
            .map(
                |datafusion::config::ConfigEntry { key, value, .. }| KeyValuePair {
                    key: key.to_owned(),
                    value: value.clone(),
                },
            )
            .collect();
        let operation_id = uuid::Uuid::now_v7().to_string();
        debug!(
            "Distributed query with session_id: {}, execution operation_id: {}",
            self.session_id, operation_id
        );
        let query = ExecuteQueryParams {
            query: Some(Query::LogicalPlan(buf)),
            settings,
            session_id: self.session_id.clone(),
            operation_id,
        };

        let metric_row_count = MetricBuilder::new(&self.metrics).output_rows(partition);
        let metric_total_bytes =
            MetricBuilder::new(&self.metrics).counter("transferred_bytes", partition);

        let stream = futures::stream::once(
            execute_query(
                self.scheduler_url.clone(),
                self.session_id.clone(),
                query,
                self.config.default_grpc_client_max_message_size(),
                GrpcClientConfig::from(&self.config),
                Arc::new(self.metrics.clone()),
                partition,
            )
            .map_err(|e| ArrowError::ExternalError(Box::new(e))),
        )
        .try_flatten()
        .inspect(move |batch| {
            metric_total_bytes.add(
                batch
                    .as_ref()
                    .map(|b| b.get_array_memory_size())
                    .unwrap_or(0),
            );

            metric_row_count.add(batch.as_ref().map(|b| b.num_rows()).unwrap_or(0));
        });

        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> Result<Statistics> {
        // This execution plan sends the logical plan to the scheduler without
        // performing the node by node conversion to a full physical plan.
        // This implies that we cannot infer the statistics at this stage.
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

async fn execute_query(
    scheduler_url: String,
    session_id: String,
    query: ExecuteQueryParams,
    max_message_size: usize,
    grpc_config: GrpcClientConfig,
    metrics: Arc<ExecutionPlanMetricsSet>,
    partition: usize,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send> {
    // Capture query submission time for total_query_time_ms
    let query_start_time = std::time::Instant::now();

    info!("Connecting to Ballista scheduler at {scheduler_url}");
    // TODO reuse the scheduler to avoid connecting to the Ballista scheduler again and again
    let connection = create_grpc_client_connection(scheduler_url, &grpc_config)
        .await
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

    let mut scheduler = SchedulerGrpcClient::new(connection)
        .max_encoding_message_size(max_message_size)
        .max_decoding_message_size(max_message_size);

    let query_result = scheduler
        .execute_query(query)
        .await
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
        .into_inner();

    let query_result = match query_result.result.unwrap() {
        execute_query_result::Result::Success(success_result) => success_result,
        execute_query_result::Result::Failure(failure_result) => {
            return Err(DataFusionError::Execution(format!(
                "Fail to execute query due to {failure_result:?}"
            )));
        }
    };

    assert_eq!(
        session_id, query_result.session_id,
        "Session id inconsistent between Client and Server side in DistributedQueryExec."
    );

    // Use shared polling logic from query_executor module
    let successful_job = query_executor::poll_job(
        &mut scheduler,
        query_result.job_id,
        Some(metrics), // Pass metrics for ExecutionPlan monitoring
        partition,
        query_start_time,
    )
    .await?;

    // Use shared partition fetching logic
    // Note: Using Flight transport (true) for compatibility with existing behavior
    query_executor::fetch_partitions(
        successful_job.partition_location,
        max_message_size,
        true,
    )
    .await
}
