extern crate schemamama;
extern crate postgres;

use postgres::error::Error as PostgresError;
use postgres::{Client, Transaction};
use schemamama::{Adapter, Migration, Version};
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

/// A migration to be used within a PostgreSQL connection.
pub trait PostgresMigration : Migration {
    /// Called when this migration is to be executed. This function has an empty body by default,
    /// so its implementation is optional.
    #[allow(unused_variables)]
    fn up(&self, transaction: &mut Transaction) -> Result<(), PostgresError> {
        Ok(())
    }

    /// Called when this migration is to be reversed. This function has an empty body by default,
    /// so its implementation is optional.
    #[allow(unused_variables)]
    fn down(&self, transaction: &mut Transaction) -> Result<(), PostgresError> {
        Ok(())
    }
}

/// An adapter that allows its migrations to act upon PostgreSQL connection transactions.
pub struct PostgresAdapter<'a> {
    client: Rc<RefCell<&'a mut Client>>,
    metadata_table: String,
}

impl<'a> PostgresAdapter<'a> {
    /// Create a new migrator tied to a PostgreSQL connection.
    pub fn new(client: &'a mut Client) -> PostgresAdapter<'a> {
        PostgresAdapter {
            client: Rc::new(RefCell::new(client)),
            metadata_table: "schemamama".into()
        }
    }

    /// Sets a custom metadata table name for this adapter. By default, the metadata table name is
    /// called `schemamama`.
    pub fn set_metadata_table<S: Into<String>>(&mut self, metadata_table: S) {
        self.metadata_table = metadata_table.into();
    }

    /// Create the tables Schemamama requires to keep track of schema state. If the tables already
    /// exist, this function has no operation.
    pub fn setup_schema(&self) -> Result<(), PostgresError> {
        let query = format!(
            "CREATE TABLE IF NOT EXISTS {} (version BIGINT PRIMARY KEY);",
            self.metadata_table,
        );
        self.client.borrow_mut().execute(query.as_str(), &[]).map(|_| ())
    }
}

impl<'a> Adapter for PostgresAdapter<'a> {
    type MigrationType = dyn PostgresMigration;
    type Error = PostgresError;

    fn current_version(&self) -> Result<Option<Version>, PostgresError> {
        let query = format!(
            "SELECT version FROM {} ORDER BY version DESC LIMIT 1;",
            self.metadata_table,
        );
        let row = self.client.borrow_mut().query(query.as_str(), &[])?;
        Ok(row.iter().next().map(|r| r.get(0)))
    }

    fn migrated_versions(&self) -> Result<BTreeSet<Version>, PostgresError> {
        let query = format!("SELECT version FROM {};", self.metadata_table);
        let row = self.client.borrow_mut().query(query.as_str(), &[])?;
        Ok(row.iter().map(|r| r.get(0)).collect())
    }

    fn apply_migration(&self, migration: &dyn PostgresMigration) -> Result<(), PostgresError> {
        let mut client = self.client.borrow_mut();
        let mut inner_tx = client.transaction()?;
        migration.up(&mut inner_tx)?;
        let query = format!("INSERT INTO {} (version) VALUES ($1);", self.metadata_table);
        inner_tx.execute(query.as_str(), &[&migration.version()]).map(|_| ())?;
        inner_tx.commit()?;
        Ok(())
    }

    fn revert_migration(&self, migration: &dyn PostgresMigration) -> Result<(), PostgresError> {
        let mut client = self.client.borrow_mut();
        let mut inner_tx = client.transaction()?;
        migration.down(&mut inner_tx)?;
        let query = format!("DELETE FROM {} WHERE version = $1;", self.metadata_table);
        inner_tx.execute(query.as_str(), &[&migration.version()]).map(|_| ())?;
        inner_tx.commit()?;
        Ok(())
    }
}
