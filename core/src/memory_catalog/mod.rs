//! Memory catalog implementation with update_table support

use std::collections::HashMap;

use async_trait::async_trait;
use futures::lock::Mutex;
use iceberg::io::FileIO;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::{Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent, TableUpdate};
use itertools::Itertools;
use uuid::Uuid;

use self::namespace_state::NamespaceState;

pub mod namespace_state;

/// namespace `location` property
const LOCATION: &str = "location";

/// Memory catalog implementation with update_table support.
#[derive(Debug)]
pub struct MemoryCatalog {
    root_namespace_state: Mutex<NamespaceState>,
    file_io: FileIO,
    warehouse_location: Option<String>,
}

impl MemoryCatalog {
    /// Creates a memory catalog.
    pub fn new(file_io: FileIO, warehouse_location: Option<String>) -> Self {
        Self {
            root_namespace_state: Mutex::new(NamespaceState::default()),
            file_io,
            warehouse_location,
        }
    }
}

#[async_trait]
impl Catalog for MemoryCatalog {
    /// List namespaces inside the catalog.
    async fn list_namespaces(
        &self,
        maybe_parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        match maybe_parent {
            None => {
                let namespaces = root_namespace_state
                    .list_top_level_namespaces()
                    .into_iter()
                    .map(|str| NamespaceIdent::new(str.to_string()))
                    .collect_vec();

                Ok(namespaces)
            }
            Some(parent_namespace_ident) => {
                let namespaces = root_namespace_state
                    .list_namespaces_under(parent_namespace_ident)?
                    .into_iter()
                    .map(|name| NamespaceIdent::new(name.to_string()))
                    .collect_vec();

                Ok(namespaces)
            }
        }
    }

    /// Create a new namespace inside the catalog.
    async fn create_namespace(
        &self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.insert_new_namespace(namespace_ident, properties.clone())?;
        let namespace = Namespace::with_properties(namespace_ident.clone(), properties);

        Ok(namespace)
    }

    /// Get a namespace information from the catalog.
    async fn get_namespace(&self, namespace_ident: &NamespaceIdent) -> Result<Namespace> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        let namespace = Namespace::with_properties(
            namespace_ident.clone(),
            root_namespace_state
                .get_properties(namespace_ident)?
                .clone(),
        );

        Ok(namespace)
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> Result<bool> {
        let guarded_namespaces = self.root_namespace_state.lock().await;

        Ok(guarded_namespaces.namespace_exists(namespace_ident))
    }

    /// Update a namespace inside the catalog.
    ///
    /// # Behavior
    ///
    /// The properties must be the full set of namespace.
    async fn update_namespace(
        &self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.replace_properties(namespace_ident, properties)
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace_ident: &NamespaceIdent) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.remove_existing_namespace(namespace_ident)
    }

    /// List tables from namespace.
    async fn list_tables(&self, namespace_ident: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        let table_names = root_namespace_state.list_tables(namespace_ident)?;
        let table_idents = table_names
            .into_iter()
            .map(|table_name| TableIdent::new(namespace_ident.clone(), table_name.clone()))
            .collect_vec();

        Ok(table_idents)
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace_ident: &NamespaceIdent,
        table_creation: TableCreation,
    ) -> Result<Table> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let table_name = table_creation.name.clone();
        let table_ident = TableIdent::new(namespace_ident.clone(), table_name);

        let (table_creation, location) = match table_creation.location.clone() {
            Some(location) => (table_creation, location),
            None => {
                let namespace_properties = root_namespace_state.get_properties(namespace_ident)?;
                let location_prefix = match namespace_properties.get(LOCATION) {
                    Some(namespace_location) => Ok(namespace_location.clone()),
                    None => match self.warehouse_location.clone() {
                        Some(warehouse_location) => Ok(format!("{}/{}", warehouse_location, namespace_ident.join("/"))),
                        None => Err(Error::new(ErrorKind::Unexpected,
                            format!(
                                "Cannot create table {:?}. No default path is set, please specify a location when creating a table.",
                                &table_ident
                            )))
                    },
                }?;

                let location = format!("{}/{}", location_prefix, table_ident.name());

                let new_table_creation = TableCreation {
                    location: Some(location.clone()),
                    ..table_creation
                };

                (new_table_creation, location)
            }
        };

        let metadata = TableMetadataBuilder::from_table_creation(table_creation)?
            .build()?
            .metadata;
        let metadata_location = format!(
            "{}/metadata/{}-{}.metadata.json",
            &location,
            0,
            Uuid::new_v4()
        );

        self.file_io
            .new_output(&metadata_location)?
            .write(serde_json::to_vec(&metadata)?.into())
            .await?;

        root_namespace_state.insert_new_table(&table_ident, metadata_location.clone())?;

        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location)
            .metadata(metadata)
            .identifier(table_ident)
            .build()
    }

    /// Load table from the catalog.
    async fn load_table(&self, table_ident: &TableIdent) -> Result<Table> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        let metadata_location = root_namespace_state.get_existing_table_location(table_ident)?;
        let input_file = self.file_io.new_input(metadata_location)?;
        let metadata_content = input_file.read().await?;
        let metadata = serde_json::from_slice::<TableMetadata>(&metadata_content)?;

        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(metadata_location.clone())
            .metadata(metadata)
            .identifier(table_ident.clone())
            .build()
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table_ident: &TableIdent) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let metadata_location = root_namespace_state.remove_existing_table(table_ident)?;
        self.file_io.delete(&metadata_location).await
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table_ident: &TableIdent) -> Result<bool> {
        let root_namespace_state = self.root_namespace_state.lock().await;

        root_namespace_state.table_exists(table_ident)
    }

    /// Rename a table in the catalog.
    async fn rename_table(
        &self,
        src_table_ident: &TableIdent,
        dst_table_ident: &TableIdent,
    ) -> Result<()> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let mut new_root_namespace_state = root_namespace_state.clone();
        let metadata_location = new_root_namespace_state
            .get_existing_table_location(src_table_ident)?
            .clone();
        new_root_namespace_state.remove_existing_table(src_table_ident)?;
        new_root_namespace_state.insert_new_table(dst_table_ident, metadata_location)?;
        *root_namespace_state = new_root_namespace_state;

        Ok(())
    }

    /// Update a table to the catalog.
    async fn update_table(&self, mut commit: TableCommit) -> Result<Table> {
        let mut root_namespace_state = self.root_namespace_state.lock().await;

        let current_metadata_location = root_namespace_state.get_existing_table_location(commit.identifier())?;
        
        // Load current metadata
        let input_file = self.file_io.new_input(current_metadata_location)?;
        let current_metadata_content = input_file.read().await?;
        let current_metadata = serde_json::from_slice::<TableMetadata>(&current_metadata_content)?;

        // For our test purposes, we'll create a simple updated metadata
        // In a real implementation, we would properly apply the commit operations
        let mut metadata_builder = TableMetadataBuilder::new_from_metadata(current_metadata, None);


        for update in commit.take_updates() {
            match update {
                TableUpdate::AddSnapshot{ snapshot}=> {
                    metadata_builder = metadata_builder.add_snapshot(snapshot)?;

                },
                TableUpdate::SetSnapshotRef{ref_name, reference} => {
                    metadata_builder = metadata_builder.set_ref(&ref_name, reference)?;
                }
                _ => {
                    unreachable!("Unsupported update type: {:?}", update);
                }
            }
        }

        // Build new metadata with a new UUID to represent the change
        let new_metadata = metadata_builder
            .assign_uuid(uuid::Uuid::new_v4())
            .build()
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to build metadata: {}", e)))?
            .metadata;

        // Generate new metadata location
        let table_location = new_metadata.location();
        let new_metadata_location = format!(
            "{}/metadata/{}-{}.metadata.json",
            &table_location,
            new_metadata.format_version(),
            Uuid::new_v4()
        );
        
        // Write new metadata
        self.file_io
            .new_output(&new_metadata_location)?
            .write(serde_json::to_vec(&new_metadata)?.into())
            .await?;
        
        // Update the table location in namespace state
        root_namespace_state.update_table_location(commit.identifier(), new_metadata_location.clone())?;
        
        // Build and return the updated table
        Table::builder()
            .file_io(self.file_io.clone())
            .metadata_location(new_metadata_location)
            .metadata(new_metadata)
            .identifier(commit.identifier().clone())
            .build()
    }
} 