extern crate schemamama;
extern crate postgres;

use schemamama::{Adapter, Migration, Version};
use std::collections::BTreeSet;

/// A migration to be used within a PostgreSQL connection.
pub trait PostgresMigration : Migration {
    /// Called when this migration is to be executed. This function has an empty body by default,
    /// so its implementation is optional.
    #[allow(unused_variables)]
    fn up(&self, transaction: &postgres::Transaction) { }

    /// Called when this migration is to be reversed. This function has an empty body by default,
    /// so its implementation is optional.
    #[allow(unused_variables)]
    fn down(&self, transaction: &postgres::Transaction) { }
}

/// An adapter that allows its migrations to act upon PostgreSQL connection transactions.
pub struct PostgresAdapter<'a> {
    connection: &'a postgres::Connection
}

impl<'a> PostgresAdapter<'a> {
    /// Create a new migrator tied to a PostgreSQL connection.
    pub fn new(connection: &'a postgres::Connection) -> PostgresAdapter {
        PostgresAdapter { connection: connection }
    }

    /// Create the tables Schemamama requires to keep track of schema state. If the tables already
    /// exist, this function has no operation.
    pub fn setup_schema(&self) {
        let query = "CREATE TABLE IF NOT EXISTS schemamama (version BIGINT PRIMARY KEY);";
        if let Err(e) = self.connection.execute(query, &[]) {
            panic!("Schema setup failed: {:?}", e);
        }
    }

    // Panics if `setup_schema` hasn't previously been called or if the insertion query otherwise
    // fails.
    fn record_version(&self, version: Version) {
        let query = "INSERT INTO schemamama (version) VALUES ($1);";
        if let Err(e) = self.connection.execute(query, &[&version]) {
            panic!("Failed to delete version {:?}: {:?}", version, e);
        }
    }

    // Panics if `setup_schema` hasn't previously been called or if the deletion query otherwise
    // fails.
    fn erase_version(&self, version: Version) {
        let query = "DELETE FROM schemamama WHERE version = $1;";
        if let Err(e) = self.connection.execute(query, &[&version]) {
            panic!("Failed to delete version {:?}: {:?}", version, e);
        }
    }

    fn execute_transaction<F>(&self, block: F) where F: Fn(&postgres::Transaction) {
        let transaction = self.connection.transaction().unwrap();
        block(&transaction);
        transaction.commit().unwrap();
    }
}

impl<'a> Adapter for PostgresAdapter<'a> {
    type MigrationType = PostgresMigration;

    /// Panics if `setup_schema` hasn't previously been called or if the query otherwise fails.
    fn current_version(&self) -> Option<Version> {
        let query = "SELECT version FROM schemamama ORDER BY version DESC LIMIT 1;";

        let statement = match self.connection.prepare(query) {
            Ok(s) => s,
            Err(e) => panic!("Schema query preperation failed: {:?}", e)
        };

        let row = match statement.query(&[]) {
            Ok(r) => r,
            Err(e) => panic!("Schema query failed: {:?}", e)
        };

        row.iter().next().map(|r| r.get(0))
    }

    /// Panics if `setup_schema` hasn't previously been called or if the query otherwise fails.
    fn migrated_versions(&self) -> BTreeSet<Version> {
        let query = "SELECT version FROM schemamama;";

        let statement = match self.connection.prepare(query) {
            Ok(s) => s,
            Err(e) => panic!("Schema query preperation failed: {:?}", e)
        };

        let row = match statement.query(&[]) {
            Ok(r) => r,
            Err(e) => panic!("Schema query failed: {:?}", e)
        };

        row.iter().map(|r| r.get(0)).collect()
    }

    /// Panics if `setup_schema` hasn't previously been called or if the migration otherwise fails.
    fn apply_migration(&self, migration: &PostgresMigration) {
        self.execute_transaction(|transaction| {
            migration.up(&transaction);
            self.record_version(migration.version());
        });
    }

    /// Panics if `setup_schema` hasn't previously been called or if the migration otherwise fails.
    fn revert_migration(&self, migration: &PostgresMigration) {
        self.execute_transaction(|transaction| {
            migration.down(&transaction);
            self.erase_version(migration.version());
        });
    }
}
