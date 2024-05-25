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

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    pub address: String,
    pub port: u16,
    pub flight_grpc_port: u16,
    pub base_url: String,
    pub data_dir: String,
    pub plugin_dir: String,
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

#[derive(Debug, Deserialize, Clone)]
pub struct StorageAws {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket: String,
    pub region: String,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageGcs {
    pub service_account_key: String,
    pub bucket: String,
    pub description: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Storage {
    Aws(StorageAws),
    Gcs(StorageGcs),
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub server: Server,
    pub session: Session,
    pub log: Log,
    pub storages: Option<Vec<Storage>>,
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
            .set_default("server.flight_grpc_port", 50051)
            .unwrap()
            .set_default("server.base_url", "/")
            .unwrap()
            .set_default("server.data_dir", "data")
            .unwrap()
            .set_default("server.plugin_dir", "plugin")
            .unwrap()
            .set_default("session.default_keep_alive", 3600)
            .unwrap()
            .set_default("session.upload_limit_size", 20) // 20MB
            .unwrap()
            .set_default("log.level", "info")
            .unwrap()
    }

    /// ## Panics
    /// Configuration variables has not been initialized.
    pub fn global() -> &'static Settings {
        LAZY_SETTINGS.get().expect("Settings is not initialized")
    }
}
