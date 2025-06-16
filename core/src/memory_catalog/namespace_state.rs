//! Namespace state management for MemoryCatalog

use std::collections::HashMap;

use iceberg::{Error, ErrorKind, NamespaceIdent, Result, TableIdent};

/// Represents the state of a namespace in the memory catalog
#[derive(Debug, Clone, Default)]
pub struct NamespaceState {
    /// Map of namespace name to its properties
    namespaces: HashMap<String, HashMap<String, String>>,
    /// Map of table identifier to its metadata location
    tables: HashMap<String, String>,
    /// Child namespaces
    children: HashMap<String, NamespaceState>,
}

impl NamespaceState {
    /// Create a new namespace state
    pub fn new() -> Self {
        Self::default()
    }

    /// List all top-level namespaces
    pub fn list_top_level_namespaces(&self) -> Vec<&String> {
        self.namespaces.keys().collect()
    }

    /// List namespaces under a given namespace
    pub fn list_namespaces_under(&self, namespace_ident: &NamespaceIdent) -> Result<Vec<&String>> {
        let namespace_state = self.get_namespace_state(namespace_ident)?;
        Ok(namespace_state.namespaces.keys().collect())
    }

    /// Check if a namespace exists
    pub fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> bool {
        self.get_namespace_state(namespace_ident).is_ok()
    }

    /// Insert a new namespace
    pub fn insert_new_namespace(
        &mut self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let parts = namespace_ident.as_ref();
        
        if parts.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Namespace identifier cannot be empty",
            ));
        }

        let mut current_state = self;
        
        // Navigate to the parent namespace
        for part in &parts[..parts.len() - 1] {
            current_state = current_state.children.entry(part.clone()).or_default();
        }
        
        let namespace_name = &parts[parts.len() - 1];
        
        if current_state.namespaces.contains_key(namespace_name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Namespace '{}' already exists", namespace_ident),
            ));
        }
        
        current_state.namespaces.insert(namespace_name.clone(), properties);
        current_state.children.insert(namespace_name.clone(), NamespaceState::new());
        
        Ok(())
    }

    /// Get properties for a namespace
    pub fn get_properties(&self, namespace_ident: &NamespaceIdent) -> Result<&HashMap<String, String>> {
        let namespace_state = self.get_namespace_state(namespace_ident)?;
        let namespace_name = namespace_ident.as_ref().last().unwrap();
        
        namespace_state.namespaces.get(namespace_name).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Namespace '{}' does not exist", namespace_ident),
            )
        })
    }

    /// Replace properties for a namespace
    pub fn replace_properties(
        &mut self,
        namespace_ident: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        let namespace_state = self.get_namespace_state_mut(namespace_ident)?;
        let namespace_name = namespace_ident.as_ref().last().unwrap();
        
        if !namespace_state.namespaces.contains_key(namespace_name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Namespace '{}' does not exist", namespace_ident),
            ));
        }
        
        namespace_state.namespaces.insert(namespace_name.clone(), properties);
        Ok(())
    }

    /// Remove an existing namespace
    pub fn remove_existing_namespace(&mut self, namespace_ident: &NamespaceIdent) -> Result<()> {
        let parts = namespace_ident.as_ref();
        
        if parts.is_empty() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Namespace identifier cannot be empty",
            ));
        }

        let mut current_state = self;
        
        // Navigate to the parent namespace
        for part in &parts[..parts.len() - 1] {
            current_state = current_state.children.get_mut(part).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Namespace '{}' does not exist", namespace_ident),
                )
            })?;
        }
        
        let namespace_name = &parts[parts.len() - 1];
        
        if !current_state.namespaces.contains_key(namespace_name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Namespace '{}' does not exist", namespace_ident),
            ));
        }
        
        // Check if namespace has tables
        if let Some(child_state) = current_state.children.get(namespace_name) {
            if !child_state.tables.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!("Namespace '{}' is not empty", namespace_ident),
                ));
            }
        }
        
        current_state.namespaces.remove(namespace_name);
        current_state.children.remove(namespace_name);
        
        Ok(())
    }

    /// List tables in a namespace
    pub fn list_tables(&self, namespace_ident: &NamespaceIdent) -> Result<Vec<&String>> {
        let namespace_state = self.get_namespace_state(namespace_ident)?;
        let child_state = namespace_state.children.get(namespace_ident.as_ref().last().unwrap())
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Namespace '{}' does not exist", namespace_ident),
                )
            })?;
        
        Ok(child_state.tables.keys().collect())
    }

    /// Insert a new table
    pub fn insert_new_table(
        &mut self,
        table_ident: &TableIdent,
        metadata_location: String,
    ) -> Result<()> {
        let namespace_state = self.get_namespace_state_mut(&table_ident.namespace)?;
        let namespace_name = table_ident.namespace.as_ref().last().unwrap();
        let child_state = namespace_state.children.get_mut(namespace_name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Namespace '{}' does not exist", table_ident.namespace),
                )
            })?;
        
        let table_name = table_ident.name();
        
        if child_state.tables.contains_key(table_name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Table '{}' already exists", table_ident),
            ));
        }
        
        child_state.tables.insert(table_name.to_string(), metadata_location);
        Ok(())
    }

    /// Get existing table location
    pub fn get_existing_table_location(&self, table_ident: &TableIdent) -> Result<&String> {
        let namespace_state = self.get_namespace_state(&table_ident.namespace)?;
        let namespace_name = table_ident.namespace.as_ref().last().unwrap();
        let child_state = namespace_state.children.get(namespace_name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Namespace '{}' does not exist", table_ident.namespace),
                )
            })?;
        
        let table_name = table_ident.name();
        
        child_state.tables.get(table_name).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Table '{}' does not exist", table_ident),
            )
        })
    }

    /// Update table location
    pub fn update_table_location(
        &mut self,
        table_ident: &TableIdent,
        new_metadata_location: String,
    ) -> Result<()> {
        let namespace_state = self.get_namespace_state_mut(&table_ident.namespace)?;
        let namespace_name = table_ident.namespace.as_ref().last().unwrap();
        let child_state = namespace_state.children.get_mut(namespace_name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Namespace '{}' does not exist", table_ident.namespace),
                )
            })?;
        
        let table_name = table_ident.name();
        
        if !child_state.tables.contains_key(table_name) {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Table '{}' does not exist", table_ident),
            ));
        }
        
        child_state.tables.insert(table_name.to_string(), new_metadata_location);
        Ok(())
    }

    /// Remove existing table
    pub fn remove_existing_table(&mut self, table_ident: &TableIdent) -> Result<String> {
        let namespace_state = self.get_namespace_state_mut(&table_ident.namespace)?;
        let namespace_name = table_ident.namespace.as_ref().last().unwrap();
        let child_state = namespace_state.children.get_mut(namespace_name)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Namespace '{}' does not exist", table_ident.namespace),
                )
            })?;
        
        let table_name = table_ident.name();
        
        child_state.tables.remove(table_name).ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Table '{}' does not exist", table_ident),
            )
        })
    }

    /// Check if table exists
    pub fn table_exists(&self, table_ident: &TableIdent) -> Result<bool> {
        match self.get_existing_table_location(table_ident) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == ErrorKind::DataInvalid => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Get namespace state for a given namespace identifier
    fn get_namespace_state(&self, namespace_ident: &NamespaceIdent) -> Result<&NamespaceState> {
        let parts = namespace_ident.as_ref();
        
        if parts.is_empty() {
            return Ok(self);
        }

        let mut current_state = self;
        
        // Navigate to the parent namespace
        for part in &parts[..parts.len() - 1] {
            current_state = current_state.children.get(part).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Namespace '{}' does not exist", namespace_ident),
                )
            })?;
        }
        
        Ok(current_state)
    }

    /// Get mutable namespace state for a given namespace identifier
    fn get_namespace_state_mut(&mut self, namespace_ident: &NamespaceIdent) -> Result<&mut NamespaceState> {
        let parts = namespace_ident.as_ref();
        
        if parts.is_empty() {
            return Ok(self);
        }

        let mut current_state = self;
        
        // Navigate to the parent namespace
        for part in &parts[..parts.len() - 1] {
            current_state = current_state.children.get_mut(part).ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Namespace '{}' does not exist", namespace_ident),
                )
            })?;
        }
        
        Ok(current_state)
    }
} 