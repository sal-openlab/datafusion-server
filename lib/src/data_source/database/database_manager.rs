// database/pool_manager.rs: Database connection pool manager
// Sasaki, Naoki <nsasaki@sal.co.jp> July 27, 2024
//

use std::collections::{hash_map::Entry, HashMap};
use std::env;
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
        database_settings: Option<&Vec<Database>>,
    ) -> Result<Self, sqlx::error::Error> {
        let mut resolvers: HashMap<String, Arc<TableResolver>> = HashMap::new();

        sqlx::any::install_default_drivers();

        Self::from_env(&mut resolvers)?;

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
                namespace.as_ref(),
                database.scheme(),
                database_name,
                &url,
                schema_cache,
                max_connections,
            )?;
        }

        Ok(())
    }

    fn from_env(
        resolvers: &mut HashMap<String, Arc<TableResolver>>,
    ) -> Result<(), sqlx::error::Error> {
        #[cfg(feature = "postgres")]
        if env::var("POSTGRES_USER").is_ok()
            && env::var("POSTGRES_PASSWORD").is_ok()
            && env::var("POSTGRES_HOST").is_ok()
            && env::var("POSTGRES_DATABASE").is_ok()
        {
            let mut url = format!(
                "postgres://{}:{}@{}:{}/{}",
                env::var("POSTGRES_USER").unwrap(),
                env::var("POSTGRES_PASSWORD").unwrap(),
                env::var("POSTGRES_HOST").unwrap(),
                env::var("POSTGRES_PORT")
                    .unwrap_or_default()
                    .parse::<u16>()
                    .unwrap_or(5432),
                env::var("POSTGRES_DATABASE").unwrap(),
            );

            if let Ok(ssl_mode) = &env::var("POSTGRES_SSL_MODE") {
                url = format!("{url}?sslmode={ssl_mode}");
            }

            Self::register(
                resolvers,
                Some(&env::var("POSTGRES_NAMESPACE").unwrap_or("postgres".to_string())),
                "postgres",
                &env::var("POSTGRES_DATABASE").unwrap(),
                &url,
                env::var("POSTGRES_ENABLE_SCHEMA_CACHE")
                    .unwrap_or_default()
                    .to_lowercase()
                    == "true",
                env::var("POSTGRES_MAX_CONNECTIONS")
                    .unwrap_or_default()
                    .parse::<u32>()
                    .unwrap_or(10),
            )?;
        }

        #[cfg(feature = "mysql")]
        if env::var("MYSQL_USER").is_ok()
            && env::var("MYSQL_PASSWORD").is_ok()
            && env::var("MYSQL_HOST").is_ok()
            && env::var("MYSQL_DATABASE").is_ok()
        {
            let mut url = format!(
                "mysql://{}:{}@{}:{}/{}",
                env::var("MYSQL_USER").unwrap(),
                env::var("MYSQL_PASSWORD").unwrap(),
                env::var("MYSQL_HOST").unwrap(),
                env::var("MYSQL_PORT")
                    .unwrap_or_default()
                    .parse::<u16>()
                    .unwrap_or(3306),
                env::var("MYSQL_DATABASE").unwrap(),
            );

            if let Ok(ssl_mode) = &env::var("MYSQL_SSL_MODE") {
                url = format!("{url}?ssl-mode={ssl_mode}");
            }

            Self::register(
                resolvers,
                Some(&env::var("MYSQL_NAMESPACE").unwrap_or("mysql".to_string())),
                "mysql",
                &env::var("MYSQL_DATABASE").unwrap(),
                &url,
                env::var("MYSQL_ENABLE_SCHEMA_CACHE")
                    .unwrap_or_default()
                    .to_lowercase()
                    == "true",
                env::var("MYSQL_MAX_CONNECTIONS")
                    .unwrap_or_default()
                    .parse::<u32>()
                    .unwrap_or(10),
            )?;
        }

        Ok(())
    }

    fn register(
        resolvers: &mut HashMap<String, Arc<TableResolver>>,
        namespace: Option<&String>,
        scheme: &str,
        database: &str,
        url: &str,
        schema_cache: bool,
        max_connections: u32,
    ) -> Result<(), sqlx::error::Error> {
        let key = namespace.map_or_else(|| scheme.to_string(), std::clone::Clone::clone);
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
