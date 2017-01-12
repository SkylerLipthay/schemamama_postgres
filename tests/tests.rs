#[macro_use]
extern crate schemamama;
extern crate schemamama_postgres;
extern crate postgres;

use schemamama::Migrator;
use schemamama_postgres::{PostgresAdapter, PostgresMigration};
use postgres::{Connection, TlsMode};
use postgres::error::Error as PostgresError;
use postgres::transaction::Transaction;

fn make_database_connection() -> Connection {
    let connection = Connection::connect("postgres://postgres@localhost", TlsMode::None).unwrap();
    connection.execute("SET search_path TO pg_temp;", &[]).unwrap();
    connection
}

fn current_schema_name(connection: &Connection) -> String {
    let query = connection.prepare("SELECT CURRENT_SCHEMA();").unwrap();
    let result = query.query(&[]).unwrap();
    result.iter().next().map(|r| r.get(0)).unwrap()
}

struct FirstMigration;
migration!(FirstMigration, 10, "first migration");

impl PostgresMigration for FirstMigration {
    fn up(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.execute("CREATE TABLE first (id BIGINT PRIMARY KEY);", &[]).map(|_| ())
    }

    fn down(&self, transaction: &Transaction) -> Result<(), PostgresError> {
        transaction.execute("DROP TABLE first;", &[]).map(|_| ())
    }
}

struct SecondMigration;
migration!(SecondMigration, 20, "second migration");

impl PostgresMigration for SecondMigration {
}

#[test]
fn test_setup() {
    let connection = make_database_connection();
    let schema_name = current_schema_name(&connection);
    let adapter = PostgresAdapter::new(&connection);
    let query = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname = $1 AND \
                 tablename = 'schemamama';";

    for _ in 0..2 {
        adapter.setup_schema().unwrap();
        assert_eq!(connection.execute(query, &[&schema_name]).unwrap(), 1);
    }
}

#[test]
fn test_migration_count() {
    let connection = make_database_connection();
    let adapter = PostgresAdapter::new(&connection);
    adapter.setup_schema().unwrap();
    let mut migrator = Migrator::new(adapter);
    migrator.register(Box::new(FirstMigration));
    migrator.register(Box::new(SecondMigration));

    migrator.up(Some(1337)).unwrap();
    assert_eq!(migrator.current_version().unwrap(), Some(20));
    migrator.down(None).unwrap();
    assert_eq!(migrator.current_version().unwrap(), None);
}

#[test]
fn test_migration_up_and_down() {
    let connection = make_database_connection();
    let schema_name = current_schema_name(&connection);
    let adapter = PostgresAdapter::new(&connection);
    adapter.setup_schema().unwrap();
    let mut migrator = Migrator::new(adapter);
    migrator.register(Box::new(FirstMigration));

    migrator.up(Some(10)).unwrap();
    let query = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname = $1 AND \
                 tablename = 'first';";
    assert_eq!(connection.execute(query, &[&schema_name]).unwrap(), 1);

    migrator.down(None).unwrap();
    assert_eq!(connection.execute(query, &[&schema_name]).unwrap(), 0);
}
