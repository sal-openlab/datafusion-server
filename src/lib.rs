#![doc = include_str!("../README.md")]
#![warn(clippy::pedantic)]

// datafusion-server - Arrow and Large Datasets Web Server
// Sasaki, Naoki <nsasaki@sal.co.jp> October 16, 2022
//

use crate::context::session_manager::SessionManager;
use clap::Parser;
use context::session_manager::SessionContextManager;
use log::Level;
use plugin::plugin_manager::{PluginManager, PLUGIN_MANAGER};
use server::http;
use settings::{Settings, LAZY_SETTINGS};
use statistics::{Statistics, LAZY_STATISTICS};
use std::path::PathBuf;
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

#[derive(Parser)]
#[clap(author, version, about = "Arrow and other large datasets web server", long_about = None)]
struct Args {
    #[clap(
        long,
        value_parser,
        short = 'f',
        value_name = "FILE",
        help = "Configuration file",
        default_value = "./config.toml"
    )]
    config: PathBuf,
}

#[allow(dead_code)]
#[tokio::main]
async fn entry() -> anyhow::Result<()> {
    let args = Args::parse();
    LAZY_SETTINGS
        .set(Settings::new(&args.config).expect("Can not initialize configurations"))
        .unwrap();
    LAZY_STATISTICS.set(Statistics::new()).unwrap();

    simple_logger::init_with_level(Settings::global().log.level().unwrap_or(Level::Info)).unwrap();

    let plugin_mgr = PluginManager::new().expect("Can not initialize plugin system");
    PLUGIN_MANAGER.set(plugin_mgr).unwrap();

    let session_mgr = Arc::new(tokio::sync::Mutex::new(SessionContextManager::new()));

    let (http_server, addr) = http::create_server::<SessionContextManager>(session_mgr.clone());
    let graceful = http_server.with_graceful_shutdown(register_shutdown_signal());

    tokio::spawn(async move {
        cleanup_worker(session_mgr).await;
    });

    log::debug!(
        "data-server v{} started, listen on {:?}",
        env!("CARGO_PKG_VERSION"),
        addr
    );
    log::debug!("with config: {:?}", Settings::global());

    if let Err(err) = graceful.await {
        log::error!("Server error: {:?}", err);
    }

    log::debug!("Server stopped");

    Ok(())
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
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    log::debug!("signal received, starting graceful shutdown");
}
