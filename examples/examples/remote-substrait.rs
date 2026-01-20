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
use ballista::extension::SubstraitSchedulerClient;
use ballista_core::extension::SessionConfigExt;
use ballista_examples::catalog::{FilesystemCatalogList, FilesystemSchemaProvider};
use ballista_examples::test_util;
use datafusion::catalog::SchemaProvider;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_substrait::serializer::serialize_bytes;
use futures::StreamExt;
use std::path::PathBuf;
use std::sync::Arc;

/// Substrait example with remote scheduler and persistent filesystem catalog.
#[tokio::main]
async fn main() -> Result<()> {
    let catalog_base_path = "/tmp/ballista_substrait_catalog";
    let catalog_list = Arc::new(FilesystemCatalogList::new(catalog_base_path)?);
    let catalog = catalog_list.register_catalog("catalog")?;

    let schema_path = PathBuf::from(catalog_base_path)
        .join("catalogs")
        .join("catalog")
        .join("schemas")
        .join("schema");
    let schema: Arc<dyn SchemaProvider> =
        Arc::new(FilesystemSchemaProvider::new(&schema_path)?);
    catalog.register_schema("schema", schema)?;

    let config = SessionConfig::new_with_ballista().with_target_partitions(4);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_catalog_list(catalog_list.clone())
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_catalog("catalog", catalog);

    let test_data = test_util::examples_test_data();

    let scheduler_url = "http://localhost:50050";

    let session_id = uuid::Uuid::now_v7();
    let client =
        SubstraitSchedulerClient::new(scheduler_url.to_string(), session_id.to_string())
            .await?;

    let ddl_plan_bytes = serialize_bytes(
        &format!(
            "CREATE EXTERNAL TABLE IF NOT EXISTS catalog.schema.test_data \
         STORED AS PARQUET \
         LOCATION '{}/alltypes_plain.parquet'",
            test_data
        ),
        &ctx,
    )
    .await?;
    client.execute_query(ddl_plan_bytes).await?;

    let substrait_plan_bytes =
        serialize_bytes("SELECT id, string_col FROM catalog.schema.test_data", &ctx)
            .await?;
    let mut stream = client.execute_query(substrait_plan_bytes).await?;

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
