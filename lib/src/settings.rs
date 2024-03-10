// settings.rs: Application wide system settings by configuration file
// Sasaki, Naoki <nsasaki@sal.co.jp> December 31, 2022
//

use std::path::Path;

use config::{
    {Config, ConfigBuilder, ConfigError, File},
    builder::DefaultState,
};
use log::Level;
use once_cell::sync::OnceCell;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Server {
    pub port: u16,
    pub base_url: String,
    pub data_dir: String,
    pub plugin_dir: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Session {
    pub default_keep_alive: i64, // in seconds
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
pub struct Settings {
    pub server: Server,
    pub session: Session,
    pub log: Log,
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
            .set_default("server.port", 4000)
            .unwrap()
            .set_default("server.base_url", "/")
            .unwrap()
            .set_default("server.data_dir", "data")
            .unwrap()
            .set_default("server.plugin_dir", "plugin")
            .unwrap()
            .set_default("session.default_keep_alive", 3600)
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
