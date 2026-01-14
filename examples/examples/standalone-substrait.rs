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
use ballista::extension::Extension;
use ballista_core::extension::SessionStateExt;
use ballista_core::serde::protobuf::execute_query_params::Query::SubstraitPlan;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{ExecuteQueryParams, execute_query_result};
use ballista_core::utils::{GrpcClientConfig, create_grpc_client_connection};
use ballista_examples::test_util;
use datafusion::execution::SessionState;
use datafusion::prelude::SessionContext;
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
    // Set up in-proc scheduler
    let scheduler_url = Extension::setup_standalone(None).await?;

    let front_end = SessionContext::new_with_state(SessionState::new_ballista_state(
        scheduler_url.clone(),
    )?);
    let session_id = front_end.session_id();

    // Connect to scheduler
    let connection = create_grpc_client_connection(
        scheduler_url.clone(),
        &GrpcClientConfig::default(),
    )
    .await
    .expect("Error creating client");
    let mut scheduler = SchedulerGrpcClient::new(connection);

    // Substitute with your favorite front-end
    let test_data = test_util::examples_test_data();
    let create_table_sql = format!(
        "CREATE EXTERNAL TABLE tbl_test STORED AS PARQUET LOCATION '{test_data}/alltypes_plain.parquet'"
    );
    let substrait_plan_bytes = serialize_bytes(&create_table_sql, &front_end).await?;

    let execute_query_params = ExecuteQueryParams {
        session_id: session_id.to_owned(),
        settings: vec![],
        operation_id: uuid::Uuid::now_v7().to_string(),
        // Substrait plan available as a plan type!
        query: Some(SubstraitPlan(substrait_plan_bytes)),
    };
    let response = scheduler
        .execute_query(execute_query_params)
        .await
        .expect("Error executing query");

    match response.into_inner().result.unwrap() {
        execute_query_result::Result::Failure(failure) => {
            return Err(DataFusionError::Execution(format!(
                "Failed to execute query: {:?}",
                failure
            )));
        }
        _ => {}
    }

    // Substitute with your favorite front-end
    let select_sql = "SELECT COUNT(1) FROM tbl_test";
    let substrait_plan_bytes = serialize_bytes(select_sql, &front_end).await?;

    let execute_query_params = ExecuteQueryParams {
        session_id: session_id.to_owned(),
        settings: vec![],
        operation_id: uuid::Uuid::now_v7().to_string(),
        // Substrait plan available as a plan type!
        query: Some(SubstraitPlan(substrait_plan_bytes)),
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
