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

use std::any::Any;
use std::collections::HashMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use async_trait::async_trait;
use base64::Engine;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

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
    base_path: PathBuf,
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
    /// # use ballista_examples::catalog::FilesystemCatalogList;
    /// let catalog_list = FilesystemCatalogList::new("/tmp/catalogs").unwrap();
    /// ```
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        let catalogs_dir = base_path.join("catalogs");

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

        catalog_list.load_catalogs()?;

        Ok(catalog_list)
    }

    /// Load all catalogs from the filesystem.
    pub fn load_catalogs(&self) -> Result<()> {
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
            if path.is_dir()
                && let Some(catalog_name) = path.file_name().and_then(|n| n.to_str())
            {
                let catalog = FilesystemCatalogProvider::new(&path)?;
                self.catalogs
                    .write()
                    .insert(catalog_name.to_string(), Arc::new(catalog));
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
    /// # use ballista_examples::catalog::FilesystemCatalogList;
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
            if path.is_dir()
                && let Some(schema_name) = path.file_name().and_then(|n| n.to_str())
            {
                let schema = FilesystemSchemaProvider::new(&path)?;
                self.schemas
                    .write()
                    .insert(schema_name.to_string(), Arc::new(schema));
            }
        }

        Ok(())
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

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let schema_path = self.catalog_path.join("schemas").join(name);
        fs::create_dir_all(&schema_path).map_err(|e| {
            DataFusionError::IoError(std::io::Error::new(
                e.kind(),
                format!("Failed to create schema directory: {}", e),
            ))
        })?;

        let old = self.schemas.write().insert(name.to_string(), schema);
        Ok(old)
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
        Ok(provider)
    }

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

    async fn create_table_from_metadata(
        &self,
        metadata: &TableMetadata,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_url = ListingTableUrl::parse(&metadata.location)?;

        let file_format: Arc<dyn FileFormat> = match metadata.file_format.as_str() {
            "parquet" => Arc::new(ParquetFormat::default()),
            "csv" => {
                let mut csv_format = CsvFormat::default();
                if let Some(delimiter) = metadata.options.get("delimiter")
                    && let Some(ch) = delimiter.chars().next()
                {
                    csv_format = csv_format.with_delimiter(ch as u8);
                }
                if let Some(has_header) = metadata.options.get("has_header")
                    && let Ok(has_header) = has_header.parse::<bool>()
                {
                    csv_format = csv_format.with_has_header(has_header);
                }
                Arc::new(csv_format)
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported file format: {}",
                    metadata.file_format
                )));
            }
        };

        let listing_options =
            ListingOptions::new(file_format).with_file_extension(&metadata.file_format);

        let config =
            ListingTableConfig::new(table_url).with_listing_options(listing_options);

        let config = if let Some(schema_base64) = &metadata.schema_json {
            let schema_bytes = base64::engine::general_purpose::STANDARD
                .decode(schema_base64)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let reader = StreamReader::try_new(Cursor::new(schema_bytes), None)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let schema = reader.schema();
            config.with_schema(schema)
        } else {
            let temp_ctx = SessionContext::new();
            config.infer_schema(&temp_ctx.state()).await?
        };

        let table = ListingTable::try_new(config)?;

        Ok(Arc::new(table))
    }

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
}

#[async_trait]
impl SchemaProvider for FilesystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables_dir = self.schema_path.join("tables");
        let mut table_names: Vec<String> = self.tables.read().keys().cloned().collect();

        if tables_dir.exists()
            && let Ok(entries) = fs::read_dir(&tables_dir)
        {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("json")
                    && let Some(name) = path.file_stem().and_then(|s| s.to_str())
                {
                    let name_string = name.to_string();
                    if !table_names.contains(&name_string) {
                        table_names.push(name_string);
                    }
                }
            }
        }

        table_names
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some(table) = self.tables.read().get(name).cloned() {
            return Ok(Some(table));
        }

        let metadata_path = self
            .schema_path
            .join("tables")
            .join(format!("{}.json", name));

        if !metadata_path.exists() {
            return Ok(None);
        }

        let metadata = self.load_table_metadata(&metadata_path)?;
        let table = self.create_table_from_metadata(&metadata).await?;
        self.tables.write().insert(name.to_string(), table.clone());

        Ok(Some(table))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let location = table
            .as_any()
            .downcast_ref::<ListingTable>()
            .and_then(|t| t.table_paths().first())
            .map(|p| p.as_str().to_string())
            .unwrap_or_default();

        let file_format = Path::new(&location)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or_else(|| panic!("Extension not found for {}", location))
            .to_string();

        let schema_json = {
            let mut buf = Vec::new();
            let mut writer = StreamWriter::try_new(&mut buf, table.schema().as_ref())
                .expect("Failed to create IPC writer");
            writer.finish().expect("Failed to finish IPC writer");
            Some(base64::engine::general_purpose::STANDARD.encode(&buf))
        };

        let metadata = TableMetadata {
            name: name.clone(),
            location,
            file_format,
            schema_json,
            options: HashMap::new(),
        };
        self.save_table_metadata(&metadata)?;

        Ok(self.tables.write().insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let table = self.tables.write().remove(name);
        let metadata_path = self
            .schema_path
            .join("tables")
            .join(format!("{}.json", name));
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

    fn table_exist(&self, name: &str) -> bool {
        if self.tables.read().contains_key(name) {
            return true;
        }

        let metadata_path = self
            .schema_path
            .join("tables")
            .join(format!("{}.json", name));
        metadata_path.exists()
    }
}
