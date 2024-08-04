// server/interval_worker.rs

use std::sync::Arc;

use crate::context::session_manager::{SessionContextManager, SessionManager};

pub async fn cleanup_and_update_metrics(
    session_mgr: Arc<tokio::sync::Mutex<SessionContextManager>>,
) {
    #[cfg(feature = "telemetry")]
    let pid = std::process::id() as usize;
    #[cfg(feature = "telemetry")]
    let mut sysinfo = sysinfo::System::new_all();
    #[cfg(feature = "telemetry")]
    sysinfo.refresh_all();

    loop {
        #[cfg(feature = "telemetry")]
        if let Some(process) = sysinfo.process(pid.into()) {
            #[allow(clippy::cast_precision_loss)]
            metrics::gauge!("memory_usage_bytes").set(process.memory() as f64);

            #[allow(clippy::cast_precision_loss)]
            metrics::gauge!("virtual_memory_usage_bytes").set(process.virtual_memory() as f64);
        }

        session_mgr.lock().await.cleanup().await;
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        #[cfg(feature = "telemetry")]
        sysinfo.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid.into()]));
    }
}
