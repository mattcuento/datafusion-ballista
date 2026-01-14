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

use ballista::datafusion::common::Result;
use ballista::prelude::SessionContextExt;
use ballista_core::extension::SessionConfigExt;
use ballista_examples::test_util;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

/// Example demonstrating scheduler-side named table registration for SQL queries.
///
/// This example shows how to:
/// 1. Register a parquet table using SQL DDL (CREATE EXTERNAL TABLE)
/// 2. Query the named table using SQL
///
/// Note: Direct Substrait plan execution via low-level gRPC currently has a limitation:
/// The scheduler creates a fresh SessionContext for each query, so tables registered in
/// one query are not available in subsequent Substrait plans sent via gRPC.
///
/// However, SQL queries work correctly because the high-level remote SessionContext API
/// maintains proper session state across queries.
///
/// For Substrait plans to work with named tables, one of these approaches is needed:
/// 1. **Use SQL API** (demonstrated here): Tables persist across SQL queries
/// 2. **Implement session persistence**: Modify scheduler to cache SessionContexts by session_id
/// 3. **Custom catalog provider**: Use `override_session_builder` to inject a catalog that
///    can resolve tables independently of session state
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
    let parquet_path = format!("{test_data}/alltypes_plain.parquet");

    // Step 1: Register table on scheduler side using SQL DDL
    println!("Step 1: Registering table 'test_data' on scheduler using SQL DDL...");

    let create_table_sql = format!(
        "CREATE EXTERNAL TABLE test_data \
         STORED AS PARQUET \
         LOCATION '{parquet_path}'"
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

    // Step 3: Execute aggregation query using the named table
    println!("Step 3: Executing aggregation query on named table...");

    let aggregation_query = "SELECT string_col, COUNT(*) as count \
                             FROM test_data \
                             WHERE id > 4 \
                             GROUP BY string_col \
                             ORDER BY string_col";

    ctx.sql(aggregation_query).await?.show().await?;
    println!("✓ Aggregation query executed successfully\n");

    println!("✅ Success! Named table 'test_data' is accessible across multiple SQL queries.");
    println!("   The scheduler maintains the table registration within the session.");

    Ok(())
}
