#[cfg(feature = "plugin")]
pub mod connector_plugin;
pub mod csv_file;
pub mod infer_schema;
pub mod json_file;
pub mod json_rest;
pub mod location_uri;
pub mod parquet;
pub mod raw_json_file;
pub mod raw_json_rest;
mod reader;
pub mod schema;
mod with_jsonpath;
pub mod writer;
