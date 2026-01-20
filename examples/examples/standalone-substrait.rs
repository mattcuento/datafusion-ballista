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

use std::sync::Arc;

use ballista::datafusion::common::Result;
use ballista::extension::{Extension, SubstraitSchedulerClient};
use ballista_core::extension::SessionConfigExt;
use ballista_examples::test_util;
use datafusion::catalog::MemoryCatalogProviderList;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_substrait::serializer::serialize_bytes;
use futures::StreamExt;

/// This example demonstrates executing Substrait plans against external tables
/// using datafusion-substrait as our frontend.
#[tokio::main]
async fn main() -> Result<()> {
    let catalog_list = Arc::new(MemoryCatalogProviderList::new());

    let config = SessionConfig::new_with_ballista();
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_catalog_list(catalog_list.clone())
        .build();
    let ctx = SessionContext::new_with_state(state.clone());

    // Use any frontend to serialize a Substrait plan
    let test_data = test_util::examples_test_data();
    let ddl_plan_bytes = serialize_bytes(
        &format!(
            "CREATE EXTERNAL TABLE IF NOT EXISTS another_data \
         STORED AS PARQUET \
         LOCATION '{}/alltypes_plain.parquet'",
            test_data
        ),
        &ctx,
    )
    .await?;
    let select_plan_bytes =
        serialize_bytes("SELECT id, string_col FROM another_data", &ctx).await?;

    let session_id = ctx.session_id();
    let scheduler_url = Extension::setup_standalone(Some(&state)).await?;
    let client =
        SubstraitSchedulerClient::new(scheduler_url, session_id.to_string()).await?;

    client.execute_query(ddl_plan_bytes).await?;
    let mut stream = client.execute_query(select_plan_bytes).await?;

    let mut batch_count = 0;
    let mut total_rows = 0;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        batch_count += 1;
        total_rows += batch.num_rows();

        println!("Batch {}: {} rows", batch_count, batch.num_rows());
        println!("{:?}", batch);
    }
    println!("---------");
    println!("Query executed successfully!");
    println!("Total batches: {}, Total rows: {}", batch_count, total_rows);

    Ok(())
}
