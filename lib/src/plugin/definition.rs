// definition.rs - plugin_def file handler.
// Sasaki, Naoki <nsasaki@sal.co.jp> February 19, 2023
//

#[cfg(feature = "plugin")]
use config::{Config, ConfigError, File};
use serde::Deserialize;
#[cfg(feature = "plugin")]
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub enum PluginType {
    #[serde(rename = "datasource")]
    DataSource,
    #[serde(rename = "processor")]
    Processor,
}

#[derive(Debug, Deserialize, Clone)]
pub struct General {
    pub title: String,
    pub plugin_type: PluginType,
    pub version: String,
    pub scheme: Option<String>,
    pub module: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Plugin {
    pub file: String,
    pub entry: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Definition {
    pub general: General,
    pub plugin: Plugin,
}

impl Definition {
    #[cfg(feature = "plugin")]
    pub fn new(def_file: &Path) -> Result<Self, ConfigError> {
        Config::builder()
            .set_default("general.version", "Unknown")?
            .set_default("plugin.file", "plugin_main.py")?
            .set_default("plugin.entry", "main")?
            .add_source(File::with_name(def_file.to_str().unwrap()))
            .build()?
            .try_deserialize()
    }
}
