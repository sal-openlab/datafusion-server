// connector_plugin - Plugin to RecordBatch, features only "plugin"
// Sasaki, Naoki <nsasaki@sal.co.jp> February 18, 2023
//

use datafusion::arrow::record_batch::RecordBatch;
use pyo3::types::PyBytes;
use pyo3::{Py, PyAny, PyResult, Python};

use crate::data_source::reader::build_record_batch;
use crate::data_source::{
    csv, location_uri, nd_json, parquet, schema::DataSourceSchema, with_jsonpath,
};
use crate::request::body::{DataSourceFormat, DataSourceOption, PluginOption};
use crate::response::http_error::ResponseError;
use crate::PluginManager;

pub fn to_record_batch(
    format: &DataSourceFormat,
    uri: &str,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
    plugin_options: &PluginOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let uri_parts =
        location_uri::to_parts(uri).map_err(|e| ResponseError::unsupported_type(e.to_string()))?;
    let uri_scheme = &uri_parts.scheme.as_ref().unwrap().to_string();
    let authority = &uri_parts.authority.as_ref().unwrap().to_string();
    let (path, query) = if let Some(pq) = &uri_parts.path_and_query {
        (
            Some(pq.path()),
            Some(location_uri::to_map(pq.query().unwrap_or(""))),
        )
    } else {
        (None, None)
    };

    let py_result = PluginManager::global().py_connector_exec(
        format.to_str(),
        uri_scheme,
        authority,
        path,
        &plugin_options.options,
        schema,
        query,
    )?;

    Ok(match format {
        DataSourceFormat::Arrow => Python::with_gil(|py| -> PyResult<Vec<RecordBatch>> {
            PluginManager::global().to_record_batches(py, &py_result)
        })
        .map_err(|e| ResponseError::python_interpreter_error(e.to_string()))?,
        DataSourceFormat::Json => {
            if options.json_path.is_none() {
                build_record_batch::from_json(&py_result.to_string(), schema, options)?
            } else {
                with_jsonpath::to_record_batch(&py_result.to_string(), schema, options)?
            }
        }
        DataSourceFormat::NdJson => nd_json::from_bytes_to_record_batch(
            bytes::Bytes::from(py_result.to_string()),
            schema,
            options,
        )?,
        DataSourceFormat::Csv => csv::from_bytes_to_record_batch(
            bytes::Bytes::from(py_result.to_string()),
            schema,
            options,
        )?,
        DataSourceFormat::Parquet => {
            parquet::from_bytes_to_record_batch(py_result_to_bytes(&py_result)?)?
        }
        #[cfg(feature = "flight")]
        DataSourceFormat::Flight => {
            return Err(ResponseError::unsupported_type(
                "Format of plugins are not supported 'flight'",
            ));
        }
        #[cfg(feature = "avro")]
        DataSourceFormat::Avro => {
            return Err(ResponseError::unsupported_type(
                "Format of plugins are not supported 'avro'",
            ));
        }
    })
}

fn py_result_to_bytes(py_result: &Py<PyAny>) -> Result<bytes::Bytes, ResponseError> {
    let mut buffer = bytes::BytesMut::new();
    Python::with_gil(|py| -> PyResult<()> {
        let py_bytes = py_result.downcast::<PyBytes>(py)?.as_bytes();
        buffer.extend_from_slice(py_bytes);
        Ok(())
    })
    .map_err(|e| {
        ResponseError::python_interpreter_error(format!("Can not downcast to buffered binary: {e}"))
    })?;
    Ok(buffer.freeze())
}
