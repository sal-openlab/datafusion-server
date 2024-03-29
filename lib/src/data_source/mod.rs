#[cfg(feature = "plugin")]
pub mod connector_plugin;
pub mod csv_file;
#[cfg(feature = "flight")]
pub mod flight_stream;
pub mod infer_schema;
pub mod json_file;
pub mod json_rest;
pub mod location_uri;
pub mod nd_json_file;
pub mod nd_json_rest;
pub mod parquet;
mod reader;
pub mod schema;
mod with_jsonpath;
pub mod writer;
