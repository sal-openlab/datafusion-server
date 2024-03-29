#![warn(clippy::pedantic)]

// datafusion-server - Arrow and Large Datasets Web Server
// Sasaki, Naoki <nsasaki@sal.co.jp> October 16, 2022
//

use std::future::IntoFuture;
use std::sync::Arc;

use log::Level;

use context::session_manager::SessionContextManager;
use plugin::plugin_manager::{PluginManager, PLUGIN_MANAGER};
#[cfg(feature = "flight")]
use server::flight;
use server::http;
use settings::{Settings, LAZY_SETTINGS};
use statistics::{Statistics, LAZY_STATISTICS};

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
/// * Python Plugin Manager (feature = "plugin" only)
/// * HTTP socket binding
/// * gRPC socket binding (feature = "flight" only)
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

    let (http_server, http_addr) =
        http::create_server::<SessionContextManager>(session_mgr.clone()).await?;

    #[cfg(feature = "flight")]
    let (flight_server, flight_addr) =
        flight::create_server::<SessionContextManager>(&session_mgr.clone())?;

    log::info!("datafusion-server v{} started", env!("CARGO_PKG_VERSION"));
    log::info!("http service listening on {http_addr:?}");
    #[cfg(feature = "flight")]
    log::info!("flight gRPC service listening on {flight_addr:?}");
    log::debug!("with config: {:?}", Settings::global());

    let http_service =
        http_server.with_graceful_shutdown(signal_handler::register_shutdown_signal());

    #[cfg(feature = "flight")]
    let flight_service = tonic::transport::Server::builder()
        .add_service(flight_server)
        .serve(flight_addr);

    #[cfg(feature = "flight")]
    tokio::select! {
        http_result = http_service.into_future() => if let Err(e) = http_result {
            log::error!("Can not initialize http server: {:?}", e);
            return Err(anyhow::anyhow!("http server initialization error: {:?}", e));
        },
        flight_result = flight_service => if let Err(e) = flight_result {
            log::error!("Can not initialize flight gRPC server: {:?}", e);
            return Err(anyhow::anyhow!("flight server initialization error: {:?}", e));
        },
        _ = cleanup_worker(session_mgr) => {},
    }

    #[cfg(not(feature = "flight"))]
    tokio::select! {
        http_result = http_service.into_future() => if let Err(e) = http_result {
            log::error!("Can not initialize http server: {:?}", e);
            return Err(anyhow::anyhow!("http server initialization error: {:?}", e));
        },
        _ = cleanup_worker(session_mgr) => {},
    }

    log::info!("Server terminated");

    Ok(())
}

async fn cleanup_worker(session_mgr: Arc<tokio::sync::Mutex<SessionContextManager>>) {
    loop {
        session_mgr.lock().await.cleanup().await;
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    }
}
