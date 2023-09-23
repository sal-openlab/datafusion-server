// connector_plugin - Plugin to RecordBatch
// Sasaki, Naoki <nsasaki@sal.co.jp> February 18, 2023
//

use crate::data_source::reader::build_record_batch;
use crate::data_source::{infer_schema, location_uri, schema::DataSourceSchema, with_jsonpath};
use crate::request::body::{DataSourceFormat, DataSourceOption, PluginOption};
use crate::response::http_error::ResponseError;
#[cfg(feature = "plugin")]
use crate::PluginManager;
use datafusion::arrow::{datatypes::SchemaRef, json, record_batch::RecordBatch};
#[cfg(feature = "plugin")]
use pyo3::{PyResult, Python};

#[cfg(feature = "plugin")]
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

    let record_batches = if options.json_path.is_none() {
        match format {
            DataSourceFormat::Json => {
                build_record_batch::from_json(&py_result.to_string(), schema, options)?
            }
            DataSourceFormat::RawJson => {
                let json_text = &py_result.to_string();

                let schema_ref = SchemaRef::new(if let Some(schema) = schema {
                    schema.to_datafusion_schema()
                } else {
                    infer_schema::from_raw_json(json_text, options)?
                });

                let mut record_batches = Vec::<RecordBatch>::new();

                let builder = json::ReaderBuilder::new(schema_ref);

                let reader = builder
                    .build(std::io::Cursor::new(&json_text))
                    .map_err(ResponseError::record_batch_creation)?;

                for record_batch in reader {
                    record_batches
                        .push(record_batch.map_err(ResponseError::record_batch_extraction)?);
                }

                record_batches
            }
            DataSourceFormat::Arrow => Python::with_gil(|py| -> PyResult<Vec<RecordBatch>> {
                PluginManager::global().to_record_batches(py, &py_result)
            })
            .map_err(|e| ResponseError::python_interpreter_error(e.to_string()))?,
            _ => {
                return Err(ResponseError::unsupported_type(
                    "Currently supported only 'json', 'rawJson' or an 'arrow'",
                ));
            }
        }
    } else {
        match format {
            DataSourceFormat::Arrow => {
                return Err(ResponseError::request_validation(
                    "Not supported JSONPath with data source format Arrow",
                ));
            }
            _ => with_jsonpath::to_record_batch(&py_result.to_string(), schema, options)?,
        }
    };

    Ok(record_batches)
}
