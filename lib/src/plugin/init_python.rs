// init_python.rs - Initializes Python binding by PyO3
// Sasaki, Naoki <nsasaki@sal.co.jp> February 15, 2023
//

#[cfg(feature = "plugin")]
use pyo3::types::PyModule;
#[cfg(feature = "plugin")]
use pyo3::Python;

#[cfg(feature = "plugin")]
pub fn py_init() -> anyhow::Result<()> {
    pyo3::prepare_freethreaded_python();

    log::debug!("Python bindings has been initialized");

    Python::with_gil(|py| {
        let sys = PyModule::import(py, "sys")?;
        let version: String = sys.getattr("version")?.extract()?;
        log::debug!("Detected runtime: {}", &version);
        Ok(())
    })
}

#[cfg(not(feature = "plugin"))]
#[allow(clippy::unnecessary_wraps)]
pub fn py_init() -> anyhow::Result<()> {
    log::info!("Python plugin system has been disabled");
    Ok(())
}
