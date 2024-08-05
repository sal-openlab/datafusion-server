// database/pool_manager.rs: Database connection pool manager
// Sasaki, Naoki <nsasaki@sal.co.jp> July 27, 2024
//

use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

use crate::data_source::database::{
    any_pool::AnyDatabasePool, engine_type::DatabaseEngineType, table_resolver::TableResolver,
};
use crate::settings::Database;

#[derive(Clone, Default)]
pub struct DatabaseManager {
    pub resolvers: HashMap<String, Arc<TableResolver>>,
}

impl DatabaseManager {
    pub fn new_with_config(
        database_settings: &Option<Vec<Database>>,
    ) -> Result<Self, sqlx::error::Error> {
        let mut resolvers: HashMap<String, Arc<TableResolver>> = HashMap::new();

        sqlx::any::install_default_drivers();

        if let Some(databases) = database_settings {
            Self::from_config(&mut resolvers, databases)?;
        }

        Ok(Self { resolvers })
    }

    fn from_config(
        resolvers: &mut HashMap<String, Arc<TableResolver>>,
        databases: &Vec<Database>,
    ) -> Result<(), sqlx::error::Error> {
        for database in databases {
            let (namespace, database_name, url, schema_cache, max_connections) = match database {
                #[cfg(feature = "postgres")]
                Database::Postgres(postgres) => {
                    let mut url = format!(
                        "{}://{}:{}@{}:{}/{}",
                        database.scheme(),
                        &postgres.user,
                        &postgres.password,
                        &postgres.host,
                        postgres.port.unwrap_or(5432),
                        &postgres.database,
                    );

                    if let Some(ssl_mode) = &postgres.ssl_mode {
                        url = format!("{url}?sslmode={ssl_mode}");
                    }

                    (
                        &postgres.namespace,
                        &postgres.database,
                        url,
                        postgres.enable_schema_cache.unwrap_or(false),
                        postgres.max_connections.unwrap_or(10),
                    )
                }
                #[cfg(feature = "mysql")]
                Database::MySQL(mysql) => {
                    let mut url = format!(
                        "{}://{}:{}@{}:{}/{}",
                        database.scheme(),
                        &mysql.user,
                        &mysql.password,
                        &mysql.host,
                        mysql.port.unwrap_or(3306),
                        &mysql.database,
                    );

                    if let Some(ssl_mode) = &mysql.ssl_mode {
                        url = format!("{url}?ssl-mode={ssl_mode}");
                    }

                    (
                        &mysql.namespace,
                        &mysql.database,
                        url,
                        mysql.enable_schema_cache.unwrap_or(false),
                        mysql.max_connections.unwrap_or(10),
                    )
                }
            };

            Self::register(
                resolvers,
                namespace,
                database.scheme(),
                database_name,
                &url,
                schema_cache,
                max_connections,
            )?;
        }

        Ok(())
    }

    fn register(
        resolvers: &mut HashMap<String, Arc<TableResolver>>,
        namespace: &Option<String>,
        scheme: &str,
        database: &str,
        url: &str,
        schema_cache: bool,
        max_connections: u32,
    ) -> Result<(), sqlx::error::Error> {
        let key = namespace.clone().unwrap_or(scheme.to_string());
        log::debug!("Create '{key}' database connection pool");

        if let Entry::Vacant(entry) = resolvers.entry(key.clone()) {
            entry.insert(Arc::new(TableResolver::new(
                DatabaseEngineType::from_scheme(scheme)?,
                AnyDatabasePool::new(url, max_connections)?,
                database,
                schema_cache,
            )));
        } else {
            log::error!("Duplicated database connection pool '{key}'");
        }

        Ok(())
    }
}
