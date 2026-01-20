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

//! Shared query execution helpers for polling job status and fetching partition data.
//!
//! These functions provide the core execution logic used by both `DistributedQueryExec`
//! (for DataFusion logical plans) and `SubstraitExec` (for Substrait plans), following
//! the DRY principle and ensuring consistent behavior across different client types.

use crate::client::BallistaClient;
use crate::serde::protobuf::{
    GetJobStatusParams, GetJobStatusResult, PartitionLocation, SuccessfulJob, job_status,
    scheduler_grpc_client::SchedulerGrpcClient,
};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use log::{error, info};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::transport::Channel;

/// Polls the scheduler until the job completes successfully or fails.
///
/// This function implements the polling loop that checks job status at regular intervals
/// (every 100ms), logging progress and optionally collecting timing metrics. It handles
/// all job states: initialization, queued, running, failed, and successful.
///
/// # Arguments
///
/// * `scheduler` - gRPC client connection to the Ballista scheduler
/// * `job_id` - Unique identifier for the job to poll
/// * `metrics` - Optional metrics collector (for ExecutionPlan integration). Pass `None` for non-ExecutionPlan clients.
/// * `partition` - Partition number for metrics tracking (used as metric tag)
/// * `query_start_time` - When the query was originally submitted (for total time calculation)
///
/// # Returns
///
/// Returns `SuccessfulJob` containing partition locations and timing metadata when the job completes.
pub async fn poll_job(
    scheduler: &mut SchedulerGrpcClient<Channel>,
    job_id: String,
    metrics: Option<Arc<ExecutionPlanMetricsSet>>,
    partition: usize,
    query_start_time: Instant,
) -> Result<SuccessfulJob> {
    let mut prev_status: Option<job_status::Status> = None;

    loop {
        let GetJobStatusResult { status } = scheduler
            .get_job_status(GetJobStatusParams {
                job_id: job_id.clone(),
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?
            .into_inner();

        let status = status.and_then(|s| s.status);
        let wait_future = tokio::time::sleep(Duration::from_millis(100));
        let has_status_change = prev_status != status;

        match status {
            None => {
                if has_status_change {
                    info!("Job {job_id} is in initialization ...");
                }
                wait_future.await;
                prev_status = status;
            }
            Some(job_status::Status::Queued(_)) => {
                if has_status_change {
                    info!("Job {job_id} is queued...");
                }
                wait_future.await;
                prev_status = status;
            }
            Some(job_status::Status::Running(_)) => {
                if has_status_change {
                    info!("Job {job_id} is running...");
                }
                wait_future.await;
                prev_status = status;
            }
            Some(job_status::Status::Failed(err)) => {
                let msg = format!("Job {} failed: {}", job_id, err.error);
                error!("{msg}");
                break Err(DataFusionError::Execution(msg));
            }
            Some(job_status::Status::Successful(successful_job)) => {
                // Calculate job execution time (server-side execution)
                let job_execution_ms = successful_job
                    .ended_at
                    .saturating_sub(successful_job.started_at);
                let duration = Duration::from_millis(job_execution_ms);

                info!("Job {job_id} finished executing in {duration:?}");

                // Calculate scheduling time (server-side queue time)
                // This includes network latency and actual queue time
                let scheduling_ms = successful_job
                    .started_at
                    .saturating_sub(successful_job.queued_at);

                // Calculate total query time (end-to-end from client perspective)
                let total_elapsed = query_start_time.elapsed();
                let total_ms = total_elapsed.as_millis();

                // Set timing metrics if provided
                if let Some(ref metrics) = metrics {
                    let metric_job_execution = MetricBuilder::new(metrics)
                        .gauge("job_execution_time_ms", partition);
                    metric_job_execution.set(job_execution_ms as usize);

                    let metric_scheduling = MetricBuilder::new(metrics)
                        .gauge("job_scheduling_in_ms", partition);
                    metric_scheduling.set(scheduling_ms as usize);

                    let metric_total_time = MetricBuilder::new(metrics)
                        .gauge("total_query_time_ms", partition);
                    metric_total_time.set(total_ms as usize);
                }

                break Ok(successful_job);
            }
        }
    }
}

/// Fetches all partition data from executors and returns a unified stream.
///
/// This function creates connections to the executors holding each partition and fetches
/// the data using either Arrow Flight (do_get) or block-based transport (do_action).
/// The streams are lazily evaluated - partitions are fetched only when the stream is consumed.
///
/// # Arguments
///
/// * `partition_locations` - List of partition locations from `SuccessfulJob`
/// * `max_message_size` - Maximum gRPC message size for executor connections
/// * `use_flight_transport` - If true, use Arrow Flight `do_get`, otherwise use block-based `do_action`
///
/// # Returns
///
/// Returns a lazy stream that yields `RecordBatch`es from all partitions in sequence.
pub async fn fetch_partitions(
    partition_locations: Vec<PartitionLocation>,
    max_message_size: usize,
    use_flight_transport: bool,
) -> Result<std::pin::Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> {
    let streams = partition_locations.into_iter().map(move |partition| {
        let f =
            fetch_partition_internal(partition, max_message_size, use_flight_transport)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)));

        futures::stream::once(f).try_flatten()
    });

    Ok(Box::pin(futures::stream::iter(streams).flatten()))
}

async fn fetch_partition_internal(
    location: PartitionLocation,
    max_message_size: usize,
    flight_transport: bool,
) -> Result<SendableRecordBatchStream> {
    let metadata = location.executor_meta.ok_or_else(|| {
        DataFusionError::Internal("Received empty executor metadata".to_owned())
    })?;
    let partition_id = location.partition_id.ok_or_else(|| {
        DataFusionError::Internal("Received empty partition id".to_owned())
    })?;

    let host = metadata.host.as_str();
    let port = metadata.port as u16;

    let mut ballista_client = BallistaClient::try_new(host, port, max_message_size)
        .await
        .map_err(|e| DataFusionError::Execution(format!("{e:?}")))?;

    ballista_client
        .fetch_partition(
            &metadata.id,
            &partition_id.into(),
            &location.path,
            host,
            port,
            flight_transport,
        )
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}
