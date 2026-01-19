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

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use ballista_examples::catalog::{FilesystemCatalogList, FilesystemSchemaProvider};
use ballista_examples::test_util;
use datafusion::catalog::{CatalogProviderList, SchemaProvider};
use datafusion::error::Result;
use datafusion::prelude::SessionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let catalog_base_path = "/tmp/ballista_filesystem_catalog";
    let catalog_list = FilesystemCatalogList::new(catalog_base_path)?;
    let catalog = catalog_list.register_catalog("catalog")?;

    let schema_path = PathBuf::from(catalog_base_path)
        .join("catalogs")
        .join("catalog")
        .join("schemas")
        .join("schema");
    let schema: Arc<dyn SchemaProvider> =
        Arc::new(FilesystemSchemaProvider::new(&schema_path)?);
    catalog.register_schema("schema", schema)?;

    let ctx = SessionContext::new();
    ctx.register_catalog("catalog", catalog);

    let test_data = test_util::examples_test_data();
    ctx.sql(
        &format!("CREATE EXTERNAL TABLE catalog.schema.all_types STORED AS PARQUET LOCATION '{test_data}/alltypes_plain.parquet'"))
        .await?;

    let df = ctx
        .sql("SELECT count(1) FROM catalog.schema.all_types")
        .await?;
    df.show().await?;

    // Reloading catalog from filesystem
    let reloaded_catalog_list = FilesystemCatalogList::new(catalog_base_path)?;
    reloaded_catalog_list.load_catalogs()?;
    assert!(
        reloaded_catalog_list
            .catalog("catalog")
            .unwrap()
            .schema("schema")
            .unwrap()
            .table_exist("all_types")
    );
    Ok(())
}
