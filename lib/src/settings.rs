// settings.rs: Application wide system settings by configuration file
// Sasaki, Naoki <nsasaki@sal.co.jp> December 31, 2022
//

use std::path::Path;

use config::{
    builder::DefaultState,
    {Config, ConfigBuilder, ConfigError, File},
};
use log::Level;
use once_cell::sync::OnceCell;
use serde::Deserialize;

#[cfg(any(feature = "postgres", feature = "mysql"))]
use crate::data_source::database::database_manager;
use crate::data_source::object_store::credential_manager;

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    pub address: String,
    pub port: u16,
    pub flight_address: String,
    pub flight_grpc_port: u16,
    pub metrics_address: String,
    pub metrics_port: u16,
    pub base_url: String,
    pub data_dir: String,
    pub plugin_dir: String,
    pub disable_stateful_features: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Session {
    pub default_keep_alive: i64,  // in seconds
    pub upload_limit_size: usize, // in MB
}

#[derive(Debug, Deserialize, Clone)]
pub struct Log {
    pub level: String,
}

impl Log {
    #[must_use]
    pub fn level(&self) -> Option<Level> {
        match &*self.level.to_lowercase() {
            "trace" => Some(Level::Trace),
            "debug" => Some(Level::Debug),
            "info" => Some(Level::Info),
            "warn" => Some(Level::Warn),
            "error" => Some(Level::Error),
            _ => None,
        }
    }
}

#[cfg(feature = "postgres")]
#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfigPostgres {
    pub namespace: Option<String>,
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: Option<u16>,
    pub database: String,
    pub ssl_mode: Option<String>,
    pub max_connections: Option<u32>,
    pub enable_schema_cache: Option<bool>,
    pub description: Option<String>,
}

#[cfg(feature = "mysql")]
#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfigMySQL {
    pub namespace: Option<String>,
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: Option<u16>,
    pub database: String,
    pub ssl_mode: Option<String>,
    pub max_connections: Option<u32>,
    pub enable_schema_cache: Option<bool>,
    pub description: Option<String>,
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Database {
    #[cfg(feature = "postgres")]
    Postgres(DatabaseConfigPostgres),
    #[cfg(feature = "mysql")]
    MySQL(DatabaseConfigMySQL),
}

#[cfg(any(feature = "postgres", feature = "mysql"))]
impl Database {
    #[must_use]
    pub fn scheme(&self) -> &str {
        match self {
            #[cfg(feature = "postgres")]
            Database::Postgres(_) => "postgres",
            #[cfg(feature = "mysql")]
            Database::MySQL(_) => "mysql",
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageAws {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket: String,
    pub region: String,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageGcp {
    pub service_account_key: String,
    pub bucket: String,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageAzure {
    pub account_name: String,
    pub access_key: String,
    pub container: String,
    pub description: Option<String>,
}

#[cfg(feature = "webdav")]
#[derive(Debug, Deserialize, Clone)]
pub struct StorageHttp {
    pub url: String,
    pub user: Option<String>,
    pub password: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Storage {
    Aws(StorageAws),
    Gcp(StorageGcp),
    Azure(StorageAzure),
    #[cfg(feature = "webdav")]
    Webdav(StorageHttp),
}

#[derive(Deserialize, Clone)]
pub struct Settings {
    pub server: Server,
    pub session: Session,
    pub log: Log,
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    pub databases: Option<Vec<Database>>,
    pub storages: Option<Vec<Storage>>,
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    #[serde(skip)]
    pub database_pool_manager: database_manager::DatabaseManager,
    #[serde(skip)]
    pub object_store_manager: credential_manager::ObjectStoreManager,
}

pub static LAZY_SETTINGS: OnceCell<Settings> = OnceCell::new();

impl Settings {
    /// ## Errors
    /// Can not convert from `Path` to UTF-8 string.
    pub fn new_with_file(config_file: &Path) -> Result<Self, ConfigError> {
        Self::defaults()
            .add_source(File::with_name(config_file.to_str().ok_or(
                ConfigError::Message(format!("Broken utf-8 file name: {config_file:#?}")),
            )?))
            .build()?
            .try_deserialize()
    }

    /// ## Errors
    /// Can not creates configuration variables.
    pub fn new() -> Result<Self, ConfigError> {
        Self::defaults().build()?.try_deserialize()
    }

    fn defaults() -> ConfigBuilder<DefaultState> {
        Config::builder()
            .set_default("server.address", "0.0.0.0")
            .unwrap()
            .set_default("server.port", 4000)
            .unwrap()
            .set_default("server.flight_address", "0.0.0.0")
            .unwrap()
            .set_default("server.flight_grpc_port", 50051)
            .unwrap()
            .set_default("server.metrics_address", "127.0.0.1")
            .unwrap()
            .set_default("server.metrics_port", 9100)
            .unwrap()
            .set_default("server.base_url", "/")
            .unwrap()
            .set_default("server.data_dir", "data")
            .unwrap()
            .set_default("server.plugin_dir", "plugin")
            .unwrap()
            .set_default("server.disable_stateful_features", false)
            .unwrap()
            .set_default("session.default_keep_alive", 3600)
            .unwrap()
            .set_default("session.upload_limit_size", 20) // 20MB
            .unwrap()
            .set_default("log.level", "info")
            .unwrap()
    }

    /// ## Errors
    /// Can not initialize object store credentials and external database connection pools.
    pub fn init_global_managers(mut self) -> Result<Self, ConfigError> {
        #[cfg(any(feature = "postgres", feature = "mysql"))]
        {
            self.database_pool_manager =
                database_manager::DatabaseManager::new_with_config(self.databases.as_ref())
                    .map_err(|e| {
                        ConfigError::Message(format!(
                            "Can not initialize database connection pools: {e}"
                        ))
                    })?;
        }

        self.object_store_manager =
            credential_manager::ObjectStoreManager::new_with_config(self.storages.as_ref())
                .map_err(|e| {
                    ConfigError::Message(format!(
                        "Can not initialize object store credentials: {e}"
                    ))
                })?;

        Ok(self)
    }

    /// ## Panics
    /// Configuration variables has not been initialized.
    pub fn global() -> &'static Settings {
        LAZY_SETTINGS.get().expect("Settings is not initialized")
    }

    #[must_use]
    pub fn debug(&self) -> String {
        let mut result = format!("{:?}, {:?}, {:?}", self.server, self.session, self.log);

        #[cfg(any(feature = "postgres", feature = "mysql"))]
        {
            let databases: Vec<_> = self
                .database_pool_manager
                .resolvers
                .keys()
                .cloned()
                .collect();
            result = format!("{result}, Database {{ namespaces: {databases:?} }}");
        }

        let stores: Vec<_> = self.object_store_manager.stores.keys().cloned().collect();
        result = format!("{result}, Storage {{ stores: {stores:?} }}");

        result
    }
}
