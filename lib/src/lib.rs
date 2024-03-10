#![warn(clippy::pedantic)]

// datafusion-server - Arrow and Large Datasets Web Server
// Sasaki, Naoki <nsasaki@sal.co.jp> October 16, 2022
//

use std::sync::Arc;

use log::Level;

use context::session_manager::SessionContextManager;
use plugin::plugin_manager::{PLUGIN_MANAGER, PluginManager};
use server::http;
use settings::{LAZY_SETTINGS, Settings};
use statistics::{LAZY_STATISTICS, Statistics};

use crate::context::session_manager::SessionManager;
use crate::server::signal_handler;

mod context;
mod data_source;
mod plugin;
mod request;
mod response;
mod server;
pub mod settings;
mod statistics;

/// ## Errors
/// Initializing errors belows:
/// * Configuration
/// * Statistics Manager
/// * Logging System
/// * DataFusion Session Manager
/// * Python Plugin Manager
/// * HTTP socket binding
///
/// ## Panics
/// * Unknown errors
#[tokio::main]
pub async fn execute(settings: Settings) -> anyhow::Result<()> {
    LAZY_SETTINGS
        .set(settings)
        .map_err(|_| anyhow::anyhow!("Can not initialize configurations"))?;

    LAZY_STATISTICS
        .set(Statistics::new())
        .map_err(|_| anyhow::anyhow!("Can not initialize statistics"))?;

    simple_logger::init_with_level(Settings::global().log.level().unwrap_or(Level::Info))?;

    let plugin_mgr = PluginManager::new()?;

    PLUGIN_MANAGER
        .set(plugin_mgr)
        .map_err(|_| anyhow::anyhow!("Can not initialize plugin manager"))?;

    let session_mgr = Arc::new(tokio::sync::Mutex::new(SessionContextManager::new()));

    let (http_server, addr) =
        http::create_server::<SessionContextManager>(session_mgr.clone()).await?;

    tokio::spawn(async move {
        cleanup_worker(session_mgr).await;
    });

    log::info!(
        "datafusion-server v{} started, listen on {:?}",
        env!("CARGO_PKG_VERSION"),
        addr
    );

    log::debug!("with config: {:?}", Settings::global());

    if let Err(err) = http_server
        .with_graceful_shutdown(signal_handler::register_shutdown_signal())
        .await
    {
        log::error!("Server error: {:?}", err);
        return Err(anyhow::anyhow!("Can not initialize http server: {:?}", err));
    }

    log::info!("Server stopped");

    Ok(())
}

async fn cleanup_worker(session_mgr: Arc<tokio::sync::Mutex<SessionContextManager>>) {
    loop {
        session_mgr.lock().await.cleanup().await;
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    }
}
