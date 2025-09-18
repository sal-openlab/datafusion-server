// plugin_map - Plugin manager.
// Sasaki, Naoki <nsasaki@sal.co.jp> February 18, 2023
//

#[cfg(feature = "plugin")]
use datafusion::arrow::{
    compute,
    pyarrow::{FromPyArrow, ToPyArrow},
    record_batch::RecordBatch,
};
use once_cell::sync::OnceCell;
#[cfg(feature = "plugin")]
use pyo3::{
    ffi::c_str,
    types::{IntoPyDict, PyAnyMethods, PyDict, PyModule},
    Bound, {Py, PyAny, PyResult, Python},
};
#[cfg(feature = "plugin")]
use std::collections::HashMap;
#[cfg(feature = "plugin")]
use std::ffi::CString;

#[cfg(feature = "plugin")]
use crate::data_source::schema::DataSourceSchema;
#[cfg(feature = "plugin")]
use crate::plugin::convert_py_data::append_to_py_dict;
use crate::plugin::{init_python, plugin_map::PluginMap};
#[cfg(feature = "plugin")]
use crate::response::http_error::ResponseError;
#[cfg(feature = "plugin")]
use crate::settings::Settings;

pub static PLUGIN_MANAGER: OnceCell<PluginManager> = OnceCell::new();

#[derive(Debug)]
pub struct PluginManager {
    #[allow(dead_code)]
    pub plugin_map: PluginMap,
}

impl PluginManager {
    pub fn new() -> anyhow::Result<Self> {
        init_python::py_init()?;
        let plugin_map = PluginMap::new()?;
        Ok(Self { plugin_map })
    }

    #[cfg(feature = "plugin")]
    pub fn global() -> &'static PluginManager {
        PLUGIN_MANAGER
            .get()
            .expect("Can not initialize plugin manager")
    }

    #[cfg(feature = "plugin")]
    pub fn registered_schemes(&self) -> Vec<String> {
        self.plugin_map.scheme_py_map.keys().cloned().collect()
    }

    #[cfg(feature = "plugin")]
    #[allow(clippy::unused_self)]
    pub fn py_interpreter_info(&self) -> String {
        Python::with_gil(|py| -> PyResult<String> {
            let sys = PyModule::import(py, "sys")?;
            sys.getattr("version")?.extract()
        })
        .unwrap_or("Unknown".to_string())
    }

    #[cfg(feature = "plugin")]
    #[allow(clippy::too_many_arguments)]
    pub fn py_connector_exec(
        &self,
        format: &str,
        scheme: &str,
        authority: &str,
        path: Option<&str>,
        plugin_options: &serde_json::Value,
        datasource_schema: Option<&DataSourceSchema>,
        query: Option<HashMap<String, String>>,
    ) -> Result<Py<PyAny>, ResponseError> {
        let (py_file, entry, _version) =
            self.plugin_map.scheme_py_map.get(scheme).ok_or_else(|| {
                ResponseError::unsupported_type(format!(
                    "Unsupported scheme '{scheme}', plugin not implemented."
                ))
            })?;

        let py_code = std::fs::read_to_string(py_file)
            .map_err(|e| ResponseError::internal_server_error(e.to_string()))?;

        let result = Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            let py_func: Py<PyAny> = PyModule::from_code(
                py,
                CString::new(py_code)?.as_c_str(),
                c_str!(""),
                c_str!(""),
            )?
            .getattr(entry.as_str())?
            .into();

            let arrow_schema = if let Some(schema) = &datasource_schema {
                Some(schema.to_arrow_schema().to_pyarrow(py)?)
            } else {
                None
            };

            let kwargs = if let Some(query) = query {
                query.into_py_dict(py)
            } else {
                Ok(PyDict::new(py))
            }?;

            append_to_py_dict(py, &[plugin_options, &self.system_info()], &kwargs)?;

            log::debug!(
                "Call py func {} with args {:?}, {:?}, {:?}, {:?}, {:?}",
                entry.as_str(),
                format,
                authority,
                path,
                arrow_schema,
                kwargs
            );

            py_func.call(py, (format, authority, path, arrow_schema), Some(&kwargs))
        })
        .map_err(|e| ResponseError::python_interpreter_error(e.to_string()))?;

        Ok(result)
    }

    #[cfg(feature = "plugin")]
    pub fn py_processor_exec(
        &self,
        record_batches: &[RecordBatch],
        module: &str,
        plugin_options: &serde_json::Value,
    ) -> Result<Vec<RecordBatch>, ResponseError> {
        if record_batches.is_empty() {
            return Err(ResponseError::request_validation(
                "Empty record batches array",
            ));
        }

        let (py_file, entry, _version) =
            self.plugin_map
                .processor_py_map
                .get(module)
                .ok_or_else(|| {
                    ResponseError::unsupported_type(format!(
                        "Not exists '{module}', plugin not implemented."
                    ))
                })?;

        let py_code = std::fs::read_to_string(py_file)
            .map_err(|e| ResponseError::internal_server_error(e.to_string()))?;
        let record_batch = compute::concat_batches(&record_batches[0].schema(), record_batches)?;
        let result = Python::with_gil(|py| -> PyResult<Vec<RecordBatch>> {
            let module = PyModule::from_code(
                py,
                CString::new(py_code)?.as_c_str(),
                c_str!(""),
                c_str!(""),
            )?;
            let py_func: Py<PyAny> = module.getattr(entry.as_str())?.into();
            let pyarrow_obj = record_batch.to_pyarrow(py)?;
            let kwargs = PyDict::new(py);

            append_to_py_dict(py, &[plugin_options, &self.system_info()], &kwargs)?;
            log::debug!("Call py func {} with args {:?}", entry.as_str(), kwargs);

            let args = (pyarrow_obj.bind(py),);
            let result = py_func.call(py, args, Some(&kwargs))?;

            self.to_record_batches(result.bind(py))
        })
        .map_err(|e| ResponseError::python_interpreter_error(e.to_string()))?;

        Ok(result)
    }

    #[cfg(feature = "plugin")]
    #[allow(clippy::unused_self)]
    pub fn to_record_batches(&self, value: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
        Ok(vec![RecordBatch::from_pyarrow_bound(value)?])
    }

    #[cfg(feature = "plugin")]
    #[allow(clippy::unused_self)]
    pub fn system_info(&self) -> serde_json::Value {
        serde_json::json!({
            "system_config": {
                "version": env!("CARGO_PKG_VERSION"),
                "log_level": &Settings::global().log.level,
                "data_dir": &Settings::global().server.data_dir,
                "plugin_dir": &Settings::global().server.plugin_dir,
            }
        })
    }
}
