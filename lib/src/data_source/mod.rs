#[cfg(feature = "plugin")]
pub mod connector_plugin;
pub mod csv;
pub mod data_type;
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub mod database;
mod decoder;
#[cfg(feature = "deltalake")]
pub mod deltalake;
#[cfg(feature = "flight")]
pub mod flight_stream;
pub mod infer_schema;
pub mod json;
pub mod local_fs;
pub mod location;
pub mod nd_json;
pub mod object_store;
pub mod parquet;
pub mod schema;
mod transport;
mod with_jsonpath;
