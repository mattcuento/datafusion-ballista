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

use ballista::datafusion::common::{DataFusionError, Result};
use ballista::prelude::SessionContextExt;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::execute_query_params::Query::SubstraitPlan;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{ExecuteQueryParams, execute_query_result};
use ballista_core::utils::{GrpcClientConfig, create_grpc_client_connection};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_substrait::serializer::serialize_bytes;

/// Example of passing Substrait plans to a remote Ballista scheduler.
///
/// This example demonstrates serializing a LogicalPlan with in-memory data (VALUES clause)
/// to Substrait format and executing it on a remote scheduler via gRPC.
///
/// datafusion-substrait is used here to serialize the plan, but any Substrait-compliant
/// front-end can be substituted.
///
/// Note: For now, Substrait plans must be submitted directly to the scheduler via gRPC client,
/// not through the regular SessionContext remote API. The implementation currently does not
/// support named table references.
#[tokio::main]
async fn main() -> Result<()> {
    // Now create the remote context for execution
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_ballista_job_name("Remote substrait Example");
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    // Serialize SQL into encoded Substrait plan. Substitute with your favorite Substrait producer.
    let substrait_bytes = serialize_bytes("SELECT 'double_field', 'string_field' FROM (VALUES (1.5, 'foo'), (2.5, 'bar'), (3.5, 'baz')) AS t(double_field, string_field)", &ctx).await?;

    let connection = create_grpc_client_connection(
        "df://localhost:50050".to_owned(),
        &GrpcClientConfig::default(),
    )
    .await
    .expect("Error creating client");
    let mut scheduler = SchedulerGrpcClient::new(connection);

    let execute_query_params = ExecuteQueryParams {
        session_id: ctx.session_id(),
        settings: vec![],
        operation_id: uuid::Uuid::now_v7().to_string(),
        // Substrait plan available as a plan type!
        query: Some(SubstraitPlan(substrait_bytes)),
    };
    let response = scheduler
        .execute_query(execute_query_params)
        .await
        .expect("Error executing query");

    match response.into_inner().result.unwrap() {
        execute_query_result::Result::Success(_) => {
            println!("Query executed successfully!");
            Ok(())
        }
        execute_query_result::Result::Failure(failure) => Err(
            DataFusionError::Execution(format!("Failed to execute query: {:?}", failure)),
        ),
    }
}
