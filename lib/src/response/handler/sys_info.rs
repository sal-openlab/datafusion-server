// sys_info.rs - System information responder
// Sasaki, Naoki <nsasaki@sal.co.jp> July 29, 2023
//

use crate::statistics::Statistics;
#[cfg(feature = "plugin")]
use crate::PluginManager;
use axum::{response::IntoResponse, Json};
use serde::Serialize;

#[cfg(feature = "plugin")]
#[derive(Serialize)]
struct PluginModule {
    module: String,
    version: String,
}

#[cfg(feature = "plugin")]
#[derive(Serialize)]
struct Plugin {
    #[serde(rename = "pythonInterpreter")]
    python_interpreter: String,
    connectors: Vec<PluginModule>,
    processors: Vec<PluginModule>,
}

#[cfg(feature = "plugin")]
impl Plugin {
    fn new(interpreter_version: String) -> Self {
        Self {
            python_interpreter: interpreter_version,
            connectors: vec![],
            processors: vec![],
        }
    }
}

#[derive(Serialize)]
struct Stats {
    #[serde(rename = "runningTime")]
    running_time: u64,
}

#[derive(Serialize)]
struct System {
    name: String,
    version: String,
    #[cfg(feature = "plugin")]
    plugin: Plugin,
    statistics: Stats,
}

#[allow(clippy::unused_async)] // requires `async` in axum
pub async fn handler() -> impl IntoResponse {
    log::info!("Accessing system information endpoint");

    #[cfg(feature = "plugin")]
    let mut plugin = Plugin::new(PluginManager::global().py_interpreter_info());

    #[cfg(feature = "plugin")]
    {
        for (name, (_path, _entry, version)) in &PluginManager::global().plugin_map.scheme_py_map {
            plugin.connectors.push(PluginModule {
                module: name.clone(),
                version: version.clone(),
            });
        }

        for (name, (_path, _entry, version)) in &PluginManager::global().plugin_map.processor_py_map
        {
            plugin.processors.push(PluginModule {
                module: name.clone(),
                version: version.clone(),
            });
        }
    }

    let statistics = Stats {
        running_time: Statistics::global()
            .server_started_at
            .elapsed()
            .unwrap()
            .as_secs(),
    };

    Json(System {
        name: env!("CARGO_PKG_NAME").to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        #[cfg(feature = "plugin")]
        plugin,
        statistics,
    })
}
