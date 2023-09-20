// settings.rs: Application wide system settings by configuration file
// Sasaki, Naoki <nsasaki@sal.co.jp> December 31, 2022
//

use config::{Config, ConfigError, File};
use log::Level;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use std::path::Path;

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
    pub fn new(config_file: &Path) -> Result<Self, ConfigError> {
        Config::builder()
            .set_default("server.port", 4000)?
            .set_default("server.base_url", "/")?
            .set_default("server.data_dir", "data")?
            .set_default("server.plugin_dir", "plugin")?
            .set_default("session.default_keep_alive", 3600)?
            .set_default("log.level", "info")?
            .add_source(File::with_name(config_file.to_str().unwrap()))
            .build()?
            .try_deserialize()
    }

    pub fn global() -> &'static Settings {
        LAZY_SETTINGS.get().expect("Settings is not initialized")
    }
}
