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
use ballista_core::serde::protobuf::{execute_query_result, ExecuteQueryParams};
use ballista_core::utils::{create_grpc_client_connection, GrpcClientConfig};
use ballista_examples::test_util;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_substrait::serializer::serialize_bytes;

/// Example demonstrating scheduler-side named table registration for Substrait plans.
///
/// This example shows how to:
/// 1. Register a parquet table on the scheduler using SQL DDL (CREATE EXTERNAL TABLE)
/// 2. Create a Substrait plan that references the named table
/// 3. Execute the Substrait plan via gRPC
///
/// This pattern enables named table references in Substrait plans by pre-registering
/// tables in the scheduler's catalog using SQL DDL statements.
///
/// Architecture:
/// - Client registers table on scheduler via SQL DDL
/// - Scheduler stores table in its session catalog
/// - Client sends Substrait plan with named table reference
/// - Scheduler resolves table name from its catalog during Substrait plan conversion
#[tokio::main]
async fn main() -> Result<()> {
    let scheduler_url = "df://localhost:50050";

    // Create remote context
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_ballista_job_name("Remote Substrait Named Table Example");

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::remote_with_state(scheduler_url, state).await?;
    let test_data = test_util::examples_test_data();

    // Step 1: Register table on scheduler side using SQL DDL
    println!("Step 1: Registering table 'test_data' on scheduler using SQL DDL...");

    let create_table_sql = format!(
        "CREATE EXTERNAL TABLE test_data \
         STORED AS PARQUET \
         LOCATION '{test_data}/alltypes_plain.parquet'"
    );

    ctx.sql(&create_table_sql).await?.show().await?;
    println!("✓ Table 'test_data' registered on scheduler\n");

    // Step 2: Verify table exists by querying it with SQL
    println!("Step 2: Verifying table registration with SQL query...");
    let verification_df = ctx
        .sql("SELECT id, string_col FROM test_data WHERE id <= 3")
        .await?;
    verification_df.show().await?;
    println!("✓ Table accessible via SQL\n");

    // Step 3: Create a Substrait plan that references the named table
    println!("Step 3: Creating Substrait plan with named table reference...");

    // This SQL query references 'test_data' which is now registered in the scheduler's catalog
    let query_with_named_table = "SELECT string_col, COUNT(*) as count \
                                  FROM test_data \
                                  WHERE id > 4 \
                                  GROUP BY string_col \
                                  ORDER BY string_col";

    let substrait_bytes = serialize_bytes(query_with_named_table, &ctx).await?;
    println!("✓ Substrait plan created (referencing 'test_data' table)\n");

    // Step 4: Execute Substrait plan via gRPC
    println!("Step 4: Executing Substrait plan via gRPC...");

    let connection = create_grpc_client_connection(
        scheduler_url.to_owned(),
        &GrpcClientConfig::default(),
    )
    .await
    .expect("Error creating gRPC client connection");

    let mut scheduler = SchedulerGrpcClient::new(connection);

    let execute_query_params = ExecuteQueryParams {
        session_id: ctx.session_id(),
        settings: vec![],
        operation_id: uuid::Uuid::now_v7().to_string(),
        query: Some(SubstraitPlan(substrait_bytes)),
    };

    let response = scheduler
        .execute_query(execute_query_params)
        .await
        .expect("Error executing Substrait query");

    match response.into_inner().result.unwrap() {
        execute_query_result::Result::Success(result) => {
            println!("✓ Query executed successfully!");
            println!("  Job ID: {}", result.job_id);
            println!("  Session ID: {}", result.session_id);
            println!("\n✅ Success! Named table references work in Substrait plans when tables are pre-registered on the scheduler.");
            Ok(())
        }
        execute_query_result::Result::Failure(failure) => Err(DataFusionError::Execution(
            format!("Failed to execute query: {:?}", failure),
        )),
    }
}
