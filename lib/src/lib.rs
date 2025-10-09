#![warn(clippy::pedantic)]

// datafusion-server - Arrow and Large Datasets Web Server
// Sasaki, Naoki <nsasaki@sal.co.jp> October 16, 2022
//

use std::future::IntoFuture;
use std::sync::Arc;

use context::session_manager::SessionContextManager;
use log::Level;
use plugin::plugin_manager::{PluginManager, PLUGIN_MANAGER};

use crate::server::{interval_worker, signal_handler};
use crate::settings::{Settings, LAZY_SETTINGS};
use crate::statistics::{Statistics, LAZY_STATISTICS};

mod context;
mod data_source;
mod plugin;
mod request;
mod response;
mod server;
pub mod settings;
mod statistics;

#[cfg(any(not(feature = "flight"), not(feature = "telemetry")))]
type BoxedFuture =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + Send>>;

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
        .set(settings.init_global_managers()?)
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
        server::http::create_server::<SessionContextManager>(session_mgr.clone()).await?;

    #[cfg(feature = "flight")]
    let (flight_server, flight_addr) =
        server::flight::create_server::<SessionContextManager>(&session_mgr.clone())?;

    #[cfg(feature = "telemetry")]
    let (metrics_server, metrics_addr) = server::metrics::create_server().await?;

    log::info!("datafusion-server v{} started", env!("CARGO_PKG_VERSION"));
    log::info!("http service listening on {http_addr:?}");
    #[cfg(feature = "flight")]
    log::info!("flight gRPC service listening on {flight_addr:?}");
    #[cfg(feature = "telemetry")]
    log::info!("metrics service listening on {metrics_addr:?}");
    log::debug!("with config: {}", Settings::global().debug());

    let http_service =
        http_server.with_graceful_shutdown(signal_handler::register_shutdown_signal());

    #[cfg(feature = "flight")]
    let flight_service = Some(Box::pin(
        tonic::transport::Server::builder()
            .add_service(flight_server)
            .serve(flight_addr),
    ));
    #[cfg(not(feature = "flight"))]
    let flight_service: Option<BoxedFuture> = None;

    #[cfg(feature = "telemetry")]
    let metrics_service = Some(metrics_server.into_future());
    #[cfg(not(feature = "telemetry"))]
    let metrics_service: Option<BoxedFuture> = None;

    tokio::select! {
        http_result = http_service.into_future() => if let Err(e) = http_result {
            log::error!("Can not initialize http server: {e:?}");
            return Err(anyhow::anyhow!("http server initialization error: {e:?}"));
        },
        flight_result = async {
            if let Some(future) = flight_service {
                future.await
            } else {
                futures::future::pending().await
            }
        } => if let Err(e) = flight_result {
            log::error!("Can not initialize flight gRPC server: {e:?}");
            return Err(anyhow::anyhow!("flight server initialization error: {e:?}"));
        },
        metrics_result = async {
            if let Some(future) = metrics_service {
                future.await
            } else {
                futures::future::pending().await
            }
        } => if let Err(e) = metrics_result {
            log::error!("Can not initialize metrics server: {e:?}");
            return Err(anyhow::anyhow!("metrics server initialization error: {e:?}"));
        },
        () = interval_worker::cleanup_and_update_metrics(session_mgr) => {},
    }

    log::info!("Server terminated");

    Ok(())
}
