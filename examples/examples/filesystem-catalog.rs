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

//! This example demonstrates how to use the filesystem-based catalog provider
//! in Ballista. Unlike the default in-memory catalog, the filesystem catalog
//! persists catalog metadata to disk, allowing it to survive across sessions.

use std::collections::HashMap;
use std::error::Error;

use ballista_core::catalog::{
    FilesystemCatalogList, FilesystemCatalogProvider, FilesystemSchemaProvider,
};
use datafusion::catalog::CatalogProviderList;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Create a temporary directory for the catalog
    // In a real application, you would use a persistent directory
    let catalog_base_path = "/tmp/ballista_filesystem_catalog";

    println!("Creating filesystem catalog at: {}", catalog_base_path);

    // Step 1: Create a FilesystemCatalogList
    // This is the top-level catalog manager that persists to disk
    let catalog_list = FilesystemCatalogList::new(catalog_base_path)?;

    println!("Filesystem catalog list created successfully!");

    // Step 2: Register a new catalog
    let catalog = catalog_list.register_catalog("my_catalog")?;
    println!("Registered catalog: 'my_catalog'");

    // Step 3: Register a schema in the catalog
    // We need to downcast to FilesystemCatalogProvider to access the register_schema method
    let fs_catalog = catalog
        .as_any()
        .downcast_ref::<FilesystemCatalogProvider>()
        .expect("Expected FilesystemCatalogProvider");

    let schema = fs_catalog.register_schema("my_schema")?;
    println!("Registered schema: 'my_schema'");

    // Step 4: Register a table in the schema
    // We need to downcast to FilesystemSchemaProvider to access the register_table method
    let fs_schema = schema
        .as_any()
        .downcast_ref::<FilesystemSchemaProvider>()
        .expect("Expected FilesystemSchemaProvider");

    // For demonstration, we'll create a sample CSV file
    let sample_csv_path = "/tmp/sample_data.csv";
    std::fs::write(
        sample_csv_path,
        "id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n",
    )?;

    println!("Created sample CSV file at: {}", sample_csv_path);

    // Register the CSV table
    let mut options = HashMap::new();
    options.insert("has_header".to_string(), "true".to_string());

    fs_schema.register_table("my_table", sample_csv_path, "csv", options)?;
    println!("Registered table: 'my_table'");

    // Step 5: Verify the catalog structure
    println!("\n--- Catalog Structure ---");
    println!("Catalogs: {:?}", catalog_list.catalog_names());

    if let Some(catalog) = catalog_list.catalog("my_catalog") {
        println!(
            "Schemas in 'my_catalog': {:?}",
            catalog.schema_names()
        );

        if let Some(schema) = catalog.schema("my_schema") {
            println!(
                "Tables in 'my_schema': {:?}",
                schema.table_names()
            );
        }
    }

    // Step 6: Use the catalog with a DataFusion SessionContext
    println!("\n--- Using with DataFusion SessionContext ---");

    let ctx = SessionContext::new();

    // Register the catalog list with the session context
    // Note: This registers the entire catalog structure
    if let Some(catalog) = catalog_list.catalog("my_catalog") {
        ctx.register_catalog("my_catalog", catalog);
    }

    // Now we can query the table using the fully qualified name
    let df = ctx
        .sql("SELECT * FROM my_catalog.my_schema.my_table")
        .await?;

    println!("Query results:");
    df.show().await?;

    // Step 7: Demonstrate persistence
    println!("\n--- Demonstrating Persistence ---");
    println!("The catalog metadata has been persisted to: {}", catalog_base_path);
    println!("You can verify this by checking the directory structure:");
    println!("  - {}/catalogs/my_catalog/", catalog_base_path);
    println!("  - {}/catalogs/my_catalog/schemas/my_schema/", catalog_base_path);
    println!("  - {}/catalogs/my_catalog/schemas/my_schema/tables/my_table.json", catalog_base_path);

    // Load the catalog again to demonstrate persistence
    println!("\nReloading catalog from filesystem...");
    let catalog_list_reloaded = FilesystemCatalogList::new(catalog_base_path)?;

    println!("Reloaded catalogs: {:?}", catalog_list_reloaded.catalog_names());
    if let Some(catalog) = catalog_list_reloaded.catalog("my_catalog") {
        println!(
            "Reloaded schemas: {:?}",
            catalog.schema_names()
        );
    }

    println!("\n✓ Filesystem catalog example completed successfully!");
    println!("\nKey Features:");
    println!("  - Catalog metadata is persisted to disk");
    println!("  - Catalogs survive across sessions");
    println!("  - Hierarchical structure: CatalogList -> Catalog -> Schema -> Table");
    println!("  - Compatible with DataFusion's catalog system");

    Ok(())
}
