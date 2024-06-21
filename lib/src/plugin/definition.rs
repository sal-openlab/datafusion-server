// definition.rs - plugin_def file handler.
// Sasaki, Naoki <nsasaki@sal.co.jp> February 19, 2023
//

#[cfg(feature = "plugin")]
use config::{Config, ConfigError, File};
#[cfg(feature = "plugin")]
use serde::Deserialize;
#[cfg(feature = "plugin")]
use std::path::Path;

#[cfg(feature = "plugin")]
#[derive(Debug, Deserialize, Clone)]
pub enum PluginType {
    #[serde(rename = "datasource")]
    DataSource,
    #[serde(rename = "processor")]
    Processor,
}

#[cfg(feature = "plugin")]
#[derive(Debug, Deserialize, Clone)]
pub struct General {
    #[allow(dead_code)]
    pub title: String,
    pub plugin_type: PluginType,
    pub version: String,
    pub scheme: Option<String>,
    pub module: Option<String>,
}

#[cfg(feature = "plugin")]
#[derive(Debug, Deserialize, Clone)]
pub struct Plugin {
    pub file: String,
    pub entry: String,
}

#[cfg(feature = "plugin")]
#[derive(Deserialize)]
pub struct Definition {
    pub general: General,
    pub plugin: Plugin,
}

#[cfg(feature = "plugin")]
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
