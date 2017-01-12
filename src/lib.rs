extern crate schemamama;
extern crate postgres;

use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;
use schemamama::{Adapter, Migration, Version};
use std::collections::BTreeSet;

/// A migration to be used within a PostgreSQL connection.
pub trait PostgresMigration : Migration {
    /// Called when this migration is to be executed. This function has an empty body by default,
    /// so its implementation is optional.
    #[allow(unused_variables)]
    fn up(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        Ok(())
    }

    /// Called when this migration is to be reversed. This function has an empty body by default,
    /// so its implementation is optional.
    #[allow(unused_variables)]
    fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        Ok(())
    }
}

/// An adapter that allows its migrations to act upon PostgreSQL connection transactions.
pub struct PostgresAdapter<'a> {
    connection: &'a postgres::GenericConnection,
}

impl<'a> PostgresAdapter<'a> {
    /// Create a new migrator tied to a PostgreSQL connection.
    pub fn new(connection: &'a postgres::GenericConnection) -> PostgresAdapter<'a> {
        PostgresAdapter { connection: connection }
    }

    /// Create the tables Schemamama requires to keep track of schema state. If the tables already
    /// exist, this function has no operation.
    pub fn setup_schema(&self) -> Result<(), PostgresError> {
        let query = "CREATE TABLE IF NOT EXISTS schemamama (version BIGINT PRIMARY KEY);";
        self.connection.execute(query, &[]).map(|_| ())
    }

    fn record_version(&self, version: Version) -> Result<(), PostgresError> {
        let query = "INSERT INTO schemamama (version) VALUES ($1);";
        self.connection.execute(query, &[&version]).map(|_| ())
    }

    fn erase_version(&self, version: Version) -> Result<(), PostgresError> {
        let query = "DELETE FROM schemamama WHERE version = $1;";
        self.connection.execute(query, &[&version]).map(|_| ())
    }
}

impl<'a> Adapter for PostgresAdapter<'a> {
    type MigrationType = PostgresMigration;
    type Error = PostgresError;

    fn current_version(&self) -> Result<Option<Version>, PostgresError> {
        let query = "SELECT version FROM schemamama ORDER BY version DESC LIMIT 1;";
        let statement = try!(self.connection.prepare(query));
        let row = try!(statement.query(&[]));
        Ok(row.iter().next().map(|r| r.get(0)))
    }

    fn migrated_versions(&self) -> Result<BTreeSet<Version>, PostgresError> {
        let query = "SELECT version FROM schemamama;";
        let statement = try!(self.connection.prepare(query));
        let row = try!(statement.query(&[]));
        Ok(row.iter().map(|r| r.get(0)).collect())
    }

    fn apply_migration(&self, migration: &PostgresMigration) -> Result<(), PostgresError> {
        let transaction = try!(self.connection.transaction());
        try!(migration.up(&transaction));
        try!(self.record_version(migration.version()));
        try!(transaction.commit());
        Ok(())
    }

    fn revert_migration(&self, migration: &PostgresMigration) -> Result<(), PostgresError> {
        let transaction = try!(self.connection.transaction());
        try!(migration.down(&transaction));
        try!(self.erase_version(migration.version()));
        try!(transaction.commit());
        Ok(())
    }
}
