#![warn(clippy::pedantic)]

// datafusion-server - Arrow and Large Datasets Web Server
// Sasaki, Naoki <nsasaki@sal.co.jp> October 16, 2022
//

use crate::context::session_manager::SessionManager;
use context::session_manager::SessionContextManager;
use log::Level;
use plugin::plugin_manager::{PluginManager, PLUGIN_MANAGER};
use server::http;
use settings::{Settings, LAZY_SETTINGS};
use statistics::{Statistics, LAZY_STATISTICS};
use std::path::Path;
use std::sync::Arc;
use tokio::signal;

mod context;
mod data_source;
mod plugin;
mod request;
mod response;
mod server;
mod settings;
mod statistics;

#[allow(clippy::missing_panics_doc)] // TODO: to be made documentation
#[tokio::main]
pub async fn execute(config_file: &Path) {
    LAZY_SETTINGS
        .set(Settings::new(config_file).expect("Can not parse arguments"))
        .expect("Can not initialize configurations");
    LAZY_STATISTICS
        .set(Statistics::new())
        .expect("Can not register statistics manager");

    simple_logger::init_with_level(Settings::global().log.level().unwrap_or(Level::Info))
        .expect("Can not initialize logger subsystem");

    let plugin_mgr = PluginManager::new().expect("Can not initialize plugin subsystem");
    PLUGIN_MANAGER
        .set(plugin_mgr)
        .expect("Can not register plugin manager");

    let session_mgr = Arc::new(tokio::sync::Mutex::new(SessionContextManager::new()));

    let (http_server, addr) = http::create_server::<SessionContextManager>(session_mgr.clone());
    let graceful = http_server.with_graceful_shutdown(register_shutdown_signal());

    tokio::spawn(async move {
        cleanup_worker(session_mgr).await;
    });

    log::info!(
        "datafusion-server v{} started, listen on {:?}",
        env!("CARGO_PKG_VERSION"),
        addr
    );
    log::debug!("with config: {:?}", Settings::global());

    if let Err(err) = graceful.await {
        log::error!("Server error: {:?}", err);
    }

    log::info!("Server stopped");
}

async fn cleanup_worker(session_mgr: Arc<tokio::sync::Mutex<SessionContextManager>>) {
    loop {
        session_mgr.lock().await.cleanup().await;
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    }
}

async fn register_shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }

    log::debug!("signal received, starting graceful shutdown");
}
