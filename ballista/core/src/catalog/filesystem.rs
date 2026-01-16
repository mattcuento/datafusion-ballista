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

//! Filesystem-based catalog provider implementation.
//!
//! This module provides catalog providers that persist metadata to the filesystem,
//! unlike the default in-memory implementations in DataFusion.
//!
//! # Directory Structure
//!
//! The filesystem catalog uses the following directory structure:
//! ```text
//! base_path/
//! ├── catalogs/
//! │   ├── catalog1/
//! │   │   ├── schemas/
//! │   │   │   ├── schema1/
//! │   │   │   │   ├── tables/
//! │   │   │   │   │   ├── table1.json
//! │   │   │   │   │   └── table2.json
//! │   │   │   └── schema2/
//! │   │   │       └── tables/
//! │   │   └── catalog.json
//! │   └── catalog2/
//! │       └── ...
//! └── catalog_list.json
//! ```
//!
//! # Table Metadata Format
//!
//! Each table is stored as a JSON file containing:
//! - Table name
//! - Schema definition (Arrow schema)
//! - File path (for file-based tables)
//! - File format (Parquet, CSV, etc.)
//! - Table options

use std::any::Any;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Metadata for a table stored in the filesystem catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Name of the table
    pub name: String,
    /// File path or directory containing table data
    pub location: String,
    /// File format (parquet, csv, etc.)
    pub file_format: String,
    /// Table schema as JSON (Arrow schema)
    pub schema_json: Option<String>,
    /// Additional options for the table
    pub options: HashMap<String, String>,
}

/// A catalog provider that persists catalog metadata to the filesystem.
///
/// This implementation stores catalog information in a directory structure on disk,
/// allowing catalogs to be persisted across sessions.
#[derive(Debug)]
pub struct FilesystemCatalogList {
    /// Base directory for storing catalog metadata
    base_path: PathBuf,
    /// Map of catalog name to catalog provider
    catalogs: Arc<RwLock<HashMap<String, Arc<dyn CatalogProvider>>>>,
}

impl FilesystemCatalogList {
    /// Create a new filesystem catalog list.
    ///
    /// # Arguments
    ///
    /// * `base_path` - Base directory where catalog metadata will be stored
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ballista_core::catalog::FilesystemCatalogList;
    /// let catalog_list = FilesystemCatalogList::new("/tmp/catalogs").unwrap();
    /// ```
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        let catalogs_dir = base_path.join("catalogs");

        // Create base directory if it doesn't exist
        fs::create_dir_all(&catalogs_dir).map_err(|e| {
            DataFusionError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to create catalogs directory: {}", e),
            ))
        })?;

        let catalog_list = Self {
            base_path: base_path.clone(),
            catalogs: Arc::new(RwLock::new(HashMap::new())),
        };

        // Load existing catalogs from filesystem
        catalog_list.load_catalogs()?;

        Ok(catalog_list)
    }

    /// Load all catalogs from the filesystem.
    fn load_catalogs(&self) -> Result<()> {
        let catalogs_dir = self.base_path.join("catalogs");

        if !catalogs_dir.exists() {
            return Ok(());
        }

        let entries = fs::read_dir(&catalogs_dir).map_err(|e| {
            DataFusionError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to read catalogs directory: {}", e),
            ))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                DataFusionError::IoError(std::io::Error::new(
                    e.kind(),
                    format!("Failed to read catalog entry: {}", e),
                ))
            })?;

            let path = entry.path();
            if path.is_dir() {
                if let Some(catalog_name) = path.file_name().and_then(|n| n.to_str()) {
                    let catalog = FilesystemCatalogProvider::new(&path)?;
                    self.catalogs
                        .write()
                        .insert(catalog_name.to_string(), Arc::new(catalog));
                }
            }
        }

        Ok(())
    }

    /// Register a new catalog.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the catalog
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use ballista_core::catalog::FilesystemCatalogList;
    /// # let catalog_list = FilesystemCatalogList::new("/tmp/catalogs").unwrap();
    /// catalog_list.register_catalog("my_catalog").unwrap();
    /// ```
    pub fn register_catalog(&self, name: &str) -> Result<Arc<dyn CatalogProvider>> {
        let catalog_path = self.base_path.join("catalogs").join(name);
        fs::create_dir_all(&catalog_path).map_err(|e| {
            DataFusionError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to create catalog directory: {}", e),
            ))
        })?;

        let catalog = Arc::new(FilesystemCatalogProvider::new(&catalog_path)?);
        self.catalogs
            .write()
            .insert(name.to_string(), catalog.clone());

        Ok(catalog)
    }
}

impl CatalogProviderList for FilesystemCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.write().insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.read().keys().cloned().collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.read().get(name).cloned()
    }
}

/// A catalog provider that persists schema metadata to the filesystem.
#[derive(Debug)]
pub struct FilesystemCatalogProvider {
    /// Path to the catalog directory
    catalog_path: PathBuf,
    /// Map of schema name to schema provider
    schemas: Arc<RwLock<HashMap<String, Arc<dyn SchemaProvider>>>>,
}

impl FilesystemCatalogProvider {
    /// Create a new filesystem catalog provider.
    ///
    /// # Arguments
    ///
    /// * `catalog_path` - Path to the catalog directory
    pub fn new<P: AsRef<Path>>(catalog_path: P) -> Result<Self> {
        let catalog_path = catalog_path.as_ref().to_path_buf();
        let schemas_dir = catalog_path.join("schemas");

        // Create schemas directory if it doesn't exist
        fs::create_dir_all(&schemas_dir).map_err(|e| {
            DataFusionError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to create schemas directory: {}", e),
            ))
        })?;

        let provider = Self {
            catalog_path: catalog_path.clone(),
            schemas: Arc::new(RwLock::new(HashMap::new())),
        };

        // Load existing schemas from filesystem
        provider.load_schemas()?;

        Ok(provider)
    }

    /// Load all schemas from the filesystem.
    fn load_schemas(&self) -> Result<()> {
        let schemas_dir = self.catalog_path.join("schemas");

        if !schemas_dir.exists() {
            return Ok(());
        }

        let entries = fs::read_dir(&schemas_dir).map_err(|e| {
            DataFusionError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to read schemas directory: {}", e),
            ))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                DataFusionError::IoError(std::io::Error::new(
                    e.kind(),
                    format!("Failed to read schema entry: {}", e),
                ))
            })?;

            let path = entry.path();
            if path.is_dir() {
                if let Some(schema_name) = path.file_name().and_then(|n| n.to_str()) {
                    let schema = FilesystemSchemaProvider::new(&path)?;
                    self.schemas
                        .write()
                        .insert(schema_name.to_string(), Arc::new(schema));
                }
            }
        }

        Ok(())
    }

    /// Register a new schema.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the schema
    pub fn register_schema(&self, name: &str) -> Result<Arc<dyn SchemaProvider>> {
        let schema_path = self.catalog_path.join("schemas").join(name);
        fs::create_dir_all(&schema_path).map_err(|e| {
            DataFusionError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to create schema directory: {}", e),
            ))
        })?;

        let schema = Arc::new(FilesystemSchemaProvider::new(&schema_path)?);
        self.schemas
            .write()
            .insert(name.to_string(), schema.clone());

        Ok(schema)
    }
}

impl CatalogProvider for FilesystemCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.read().keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.read().get(name).cloned()
    }
}

/// A schema provider that persists table metadata to the filesystem.
#[derive(Debug)]
pub struct FilesystemSchemaProvider {
    /// Path to the schema directory
    schema_path: PathBuf,
    /// Map of table name to table provider
    tables: Arc<RwLock<HashMap<String, Arc<dyn TableProvider>>>>,
}

impl FilesystemSchemaProvider {
    /// Create a new filesystem schema provider.
    ///
    /// # Arguments
    ///
    /// * `schema_path` - Path to the schema directory
    pub fn new<P: AsRef<Path>>(schema_path: P) -> Result<Self> {
        let schema_path = schema_path.as_ref().to_path_buf();
        let tables_dir = schema_path.join("tables");

        // Create tables directory if it doesn't exist
        fs::create_dir_all(&tables_dir).map_err(|e| {
            DataFusionError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to create tables directory: {}", e),
            ))
        })?;

        let provider = Self {
            schema_path: schema_path.clone(),
            tables: Arc::new(RwLock::new(HashMap::new())),
        };

        // Load existing tables from filesystem
        provider.load_tables()?;

        Ok(provider)
    }

    /// Load all tables from the filesystem.
    fn load_tables(&self) -> Result<()> {
        let tables_dir = self.schema_path.join("tables");

        if !tables_dir.exists() {
            return Ok(());
        }

        let entries = fs::read_dir(&tables_dir).map_err(|e| {
            DataFusionError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to read tables directory: {}", e),
            ))
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                DataFusionError::IoError(std::io::Error::new(
                    e.kind(),
                    format!("Failed to read table entry: {}", e),
                ))
            })?;

            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let metadata = self.load_table_metadata(&path)?;
                let table = self.create_table_from_metadata(&metadata)?;
                self.tables
                    .write()
                    .insert(metadata.name.clone(), table);
            }
        }

        Ok(())
    }

    /// Load table metadata from a JSON file.
    fn load_table_metadata(&self, path: &Path) -> Result<TableMetadata> {
        let content = fs::read_to_string(path).map_err(|e| {
            DataFusionError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to read table metadata: {}", e),
            ))
        })?;

        serde_json::from_str(&content).map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to parse table metadata: {}", e),
            )))
        })
    }

    /// Create a table provider from metadata.
    fn create_table_from_metadata(
        &self,
        metadata: &TableMetadata,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_url = ListingTableUrl::parse(&metadata.location)?;

        let file_format: Arc<dyn FileFormat> = match metadata.file_format.as_str() {
            "parquet" => Arc::new(ParquetFormat::default()),
            "csv" => {
                let mut csv_format = CsvFormat::default();
                // Apply CSV-specific options from metadata.options if needed
                if let Some(delimiter) = metadata.options.get("delimiter") {
                    if let Some(ch) = delimiter.chars().next() {
                        csv_format = csv_format.with_delimiter(ch as u8);
                    }
                }
                if let Some(has_header) = metadata.options.get("has_header") {
                    if let Ok(has_header) = has_header.parse::<bool>() {
                        csv_format = csv_format.with_has_header(has_header);
                    }
                }
                Arc::new(csv_format)
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported file format: {}",
                    metadata.file_format
                )))
            }
        };

        let listing_options =
            ListingOptions::new(file_format).with_file_extension(&metadata.file_format);

        let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);

        let table = ListingTable::try_new(config)?;

        Ok(Arc::new(table))
    }

    /// Save table metadata to a JSON file.
    fn save_table_metadata(&self, metadata: &TableMetadata) -> Result<()> {
        let tables_dir = self.schema_path.join("tables");
        let metadata_path = tables_dir.join(format!("{}.json", metadata.name));

        let content = serde_json::to_string_pretty(metadata).map_err(|e| {
            DataFusionError::External(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize table metadata: {}", e),
            )))
        })?;

        fs::write(&metadata_path, content).map_err(|e| {
            DataFusionError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to write table metadata: {}", e),
            ))
        })?;

        Ok(())
    }

    /// Register a table from a file or directory.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the table
    /// * `location` - File path or directory containing table data
    /// * `file_format` - File format (parquet, csv, etc.)
    /// * `options` - Additional table options
    pub fn register_table(
        &self,
        name: &str,
        location: &str,
        file_format: &str,
        options: HashMap<String, String>,
    ) -> Result<()> {
        let metadata = TableMetadata {
            name: name.to_string(),
            location: location.to_string(),
            file_format: file_format.to_string(),
            schema_json: None,
            options,
        };

        // Save metadata to filesystem
        self.save_table_metadata(&metadata)?;

        // Create and register table provider
        let table = self.create_table_from_metadata(&metadata)?;
        self.tables.write().insert(name.to_string(), table);

        Ok(())
    }

    /// Deregister a table.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the table to deregister
    pub fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // Remove from in-memory map
        let table = self.tables.write().remove(name);

        // Remove metadata file
        let metadata_path = self.schema_path.join("tables").join(format!("{}.json", name));
        if metadata_path.exists() {
            fs::remove_file(&metadata_path).map_err(|e| {
                DataFusionError::IoError(std::io::Error::new(
                    e.kind(),
                    format!("Failed to remove table metadata: {}", e),
                ))
            })?;
        }

        Ok(table)
    }
}

#[async_trait]
impl SchemaProvider for FilesystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.read().get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_filesystem_catalog_list_creation() {
        let temp_dir = TempDir::new().unwrap();
        let catalog_list = FilesystemCatalogList::new(temp_dir.path()).unwrap();

        assert_eq!(catalog_list.catalog_names().len(), 0);
    }

    #[test]
    fn test_register_catalog() {
        let temp_dir = TempDir::new().unwrap();
        let catalog_list = FilesystemCatalogList::new(temp_dir.path()).unwrap();

        let _catalog = catalog_list.register_catalog("test_catalog").unwrap();
        assert!(catalog_list.catalog("test_catalog").is_some());
        assert_eq!(catalog_list.catalog_names().len(), 1);
    }

    #[test]
    fn test_catalog_persistence() {
        let temp_dir = TempDir::new().unwrap();

        // Create and register a catalog
        {
            let catalog_list = FilesystemCatalogList::new(temp_dir.path()).unwrap();
            catalog_list.register_catalog("test_catalog").unwrap();
        }

        // Load from filesystem and verify
        {
            let catalog_list = FilesystemCatalogList::new(temp_dir.path()).unwrap();
            assert_eq!(catalog_list.catalog_names().len(), 1);
            assert!(catalog_list.catalog("test_catalog").is_some());
        }
    }

    #[test]
    fn test_register_schema() {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("test_catalog");
        let catalog = FilesystemCatalogProvider::new(&catalog_path).unwrap();

        let _schema = catalog.register_schema("test_schema").unwrap();
        assert!(catalog.schema("test_schema").is_some());
        assert_eq!(catalog.schema_names().len(), 1);
    }

    #[test]
    fn test_schema_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("test_catalog");

        // Create and register a schema
        {
            let catalog = FilesystemCatalogProvider::new(&catalog_path).unwrap();
            catalog.register_schema("test_schema").unwrap();
        }

        // Load from filesystem and verify
        {
            let catalog = FilesystemCatalogProvider::new(&catalog_path).unwrap();
            assert_eq!(catalog.schema_names().len(), 1);
            assert!(catalog.schema("test_schema").is_some());
        }
    }
}
