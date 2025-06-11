// body.rs - Request body definitions for dataframe queries
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use std::collections::HashMap;

use serde::Deserialize;

use crate::context::variable::SessionVariable;
use crate::data_source::{
    location::{self, uri::SupportedScheme},
    schema,
};
use crate::response::http_error::ResponseError;

#[derive(Deserialize, Clone, Debug)]
pub struct DataSourceOption {
    #[serde(rename = "hasHeader")]
    pub has_header: Option<bool>,
    #[serde(rename = "inferSchemaRows")]
    pub infer_schema_rows: Option<usize>,
    pub delimiter: Option<char>,
    #[serde(rename = "jsonPath")]
    pub json_path: Option<String>,
    #[serde(rename = "requireNormalize")]
    pub require_normalize: Option<bool>,
    pub overwrite: Option<bool>,
    // for http headers
    pub headers: Option<HashMap<String, String>>,
    #[allow(dead_code)]
    pub version: Option<u64>,
}

impl DataSourceOption {
    /// Creates a data source options
    pub fn new() -> Self {
        Self {
            has_header: None,
            infer_schema_rows: None,
            delimiter: None,
            json_path: None,
            require_normalize: None,
            overwrite: None,
            headers: None,
            version: None,
        }
    }

    /// Creates a data source options with defaults
    pub fn default() -> Self {
        Self {
            has_header: Some(true),
            infer_schema_rows: Some(100),
            delimiter: Some(','),
            json_path: None,
            require_normalize: Some(false),
            overwrite: Some(false),
            headers: None,
            version: None,
        }
    }

    pub fn with_infer_schema_rows(mut self, rows: usize) -> Self {
        self.infer_schema_rows = Some(rows);
        self
    }
}

#[cfg(feature = "plugin")]
#[derive(Deserialize, Clone, Debug)]
#[serde(transparent)]
pub struct PluginOption {
    pub options: serde_json::Value,
}

#[cfg(feature = "plugin")]
impl PluginOption {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            options: serde_json::json!("{}"),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub enum DataSourceFormat {
    #[serde(rename = "csv")]
    Csv,
    #[serde(rename = "json")]
    Json,
    #[serde(rename = "ndJson")]
    NdJson,
    #[serde(rename = "parquet")]
    Parquet,
    #[cfg(feature = "avro")]
    #[serde(rename = "avro")]
    Avro,
    #[serde(rename = "arrow")]
    Arrow,
    #[cfg(feature = "flight")]
    #[serde(rename = "flight")]
    Flight,
    #[cfg(feature = "deltalake")]
    #[serde(rename = "deltalake")]
    Deltalake,
}

impl DataSourceFormat {
    #[allow(dead_code)]
    pub fn to_str(&self) -> &'static str {
        match self {
            DataSourceFormat::Csv => "csv",
            DataSourceFormat::Json => "json",
            DataSourceFormat::NdJson => "ndJson",
            DataSourceFormat::Parquet => "parquet",
            DataSourceFormat::Arrow => "arrow",
            #[cfg(feature = "avro")]
            DataSourceFormat::Avro => "avro",
            #[cfg(feature = "flight")]
            DataSourceFormat::Flight => "flight",
            #[cfg(feature = "deltalake")]
            DataSourceFormat::Deltalake => "deltaLake",
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct DataSource {
    pub format: DataSourceFormat,
    pub name: String,
    pub location: String,
    pub schema: Option<schema::DataSourceSchema>,
    pub options: Option<DataSourceOption>,
    #[cfg(feature = "plugin")]
    #[serde(rename = "pluginOptions")]
    pub plugin_options: Option<PluginOption>,
}

impl DataSource {
    pub fn new(format: DataSourceFormat, name: &str, location: Option<&str>) -> Self {
        Self {
            format,
            name: name.to_string(),
            location: location.unwrap_or("").to_string(),
            schema: None,
            options: None,
            #[cfg(feature = "plugin")]
            plugin_options: None,
        }
    }

    #[allow(dead_code)] // TODO: Delete if not likely to be used in the future
    pub fn with_schema(mut self, schema: schema::DataSourceSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    #[allow(dead_code)] // TODO: Delete if not likely to be used in the future
    pub fn with_options(mut self, options: DataSourceOption) -> Self {
        self.options = Some(options);
        self
    }

    #[allow(dead_code)] // TODO: Delete if not likely to be used in the future
    #[cfg(feature = "plugin")]
    pub fn with_plugin_options(mut self, options: PluginOption) -> Self {
        self.plugin_options = Some(options);
        self
    }

    pub fn validator(&self) -> Result<(), ResponseError> {
        let uri = location::uri::to_parts(&self.location)
            .map_err(|e| ResponseError::unsupported_type(e.to_string()))?;
        let scheme = location::uri::scheme(&uri)?;

        match self.format {
            DataSourceFormat::Csv => {}
            DataSourceFormat::Json => {}
            DataSourceFormat::NdJson => {
                if self.options.is_some() && self.options.as_ref().unwrap().json_path.is_some() {
                    return Err(ResponseError::unsupported_type(
                        "Not supported data source option, ndJson with JSONPath",
                    ));
                }
            }
            DataSourceFormat::Parquet => {}
            DataSourceFormat::Arrow => {
                if scheme == SupportedScheme::File {
                    return Err(ResponseError::unsupported_type(
                        "Not supported data source format, 'arrow' only use for in-memory connector",
                    ));
                }
            }
            #[cfg(feature = "avro")]
            DataSourceFormat::Avro => {
                if !scheme.handle_object_store() {
                    return Err(ResponseError::unsupported_type(format!(
                        "Not supported data source, Avro with remote location '{}'",
                        self.location
                    )));
                }
            }
            #[cfg(feature = "flight")]
            DataSourceFormat::Flight => {
                if !matches!(
                    scheme,
                    SupportedScheme::Grpc
                        | SupportedScheme::GrpcTls
                        | SupportedScheme::Http
                        | SupportedScheme::Https
                ) {
                    return Err(ResponseError::unsupported_type(
                        "Data source flight only supported 'http', 'https', 'grpc', 'grpc+tls' schemes",
                    ));
                }
            }
            #[cfg(feature = "deltalake")]
            DataSourceFormat::Deltalake => {
                #[cfg(feature = "flight")]
                if matches!(scheme, SupportedScheme::Grpc | SupportedScheme::GrpcTls) {
                    return Err(ResponseError::unsupported_type(
                        "Data source delta lake not supported 'grpc' and 'grpc+tls' schemes",
                    ));
                }
            }
        }

        Ok(())
    }
}

#[derive(Deserialize, Clone, Debug)]
#[serde(transparent)]
pub struct DataSources {
    pub data_sources: Vec<DataSource>,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(transparent)]
pub struct Variables {
    pub variables: Vec<SessionVariable>,
}

#[derive(Deserialize, Clone, Debug)]
pub enum MergeDirection {
    #[serde(rename = "column")]
    Column,
    #[serde(rename = "row")]
    Row,
}

#[derive(Deserialize, Clone, Debug)]
pub struct MergeTargets {
    #[serde(rename = "table")]
    pub table_name: String,
    #[serde(rename = "baseKeys")]
    pub base_keys: Vec<String>,
    #[serde(rename = "targetKeys")]
    pub target_keys: Vec<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct MergeOption {
    pub distinct: Option<bool>,
    #[serde(rename = "removeAfterMerged")]
    pub remove_after_merged: Option<bool>,
}

impl MergeOption {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            distinct: Some(false),
            remove_after_merged: Some(false),
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct MergeProcessor {
    pub direction: MergeDirection,
    #[serde(rename = "baseTable")]
    pub base_table_name: String,
    pub targets: Option<Vec<MergeTargets>>,
    // for column direction merge
    #[serde(rename = "targetTables")]
    pub target_table_names: Option<Vec<String>>,
    // for row direction merge
    pub options: Option<MergeOption>,
}

impl MergeProcessor {
    pub fn validator(&self) -> Result<(), ResponseError> {
        match self.direction {
            MergeDirection::Column => {
                if let Some(targets) = &self.targets {
                    for target in targets {
                        if target.base_keys.len() != target.target_keys.len() {
                            return Err(ResponseError::request_validation(
                                "Not matches count of base and target keys",
                            ));
                        }
                    }
                } else {
                    return Err(ResponseError::request_validation(
                        "Must be required 'targets' in merges column direction",
                    ));
                }
            }
            MergeDirection::Row => {
                if self.target_table_names.is_none() {
                    return Err(ResponseError::request_validation(
                        "Must be required 'targetTables' in merges row direction",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct Processor {
    #[serde(rename = "mergeProcessors")]
    pub merge_processors: Option<Vec<MergeProcessor>>,
}

#[cfg(feature = "plugin")]
#[derive(Deserialize, Clone, Debug)]
pub struct PostProcessor {
    pub module: Option<String>,
    // TODO: temporarily disabled for security reasons
    // #[serde(rename = "pythonScript")]
    // pub python_script_code: Option<String>,
    #[serde(rename = "pluginOptions")]
    pub plugin_options: Option<PluginOption>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct QueryLanguage {
    pub sql: String,
    #[cfg(feature = "plugin")]
    #[serde(rename = "postProcessors")]
    pub post_processors: Option<Vec<PostProcessor>>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ResponseFormatOption {
    #[serde(rename = "hasHeaders")]
    pub has_headers: Option<bool>,
    pub delimiter: Option<char>,
}

impl ResponseFormatOption {
    pub fn new() -> Self {
        Self {
            has_headers: Some(true),
            delimiter: Some(','),
        }
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub enum ResponseFormat {
    #[serde(rename = "arrow")]
    Arrow,
    #[serde(rename = "json")]
    Json,
    #[serde(rename = "csv")]
    Csv,
}

#[derive(Deserialize, Clone, Debug)]
pub struct QueryResponse {
    pub format: ResponseFormat,
    pub options: Option<ResponseFormatOption>,
}

impl QueryResponse {
    pub fn new() -> Self {
        Self {
            format: ResponseFormat::Json,
            options: None,
        }
    }

    pub fn new_with_format(format: ResponseFormat) -> Self {
        Self {
            format,
            options: None,
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct DataFrameQuery {
    #[serde(rename = "dataSources")]
    pub data_sources: Vec<DataSource>,
    pub variables: Option<Variables>,
    pub processor: Option<Processor>,
    #[serde(rename = "query")]
    pub query_lang: QueryLanguage,
    pub response: Option<QueryResponse>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct QueryWithResponseFormat {
    #[serde(rename = "query")]
    pub query_lang: QueryLanguage,
    pub response: Option<QueryResponse>,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum SessionQuery {
    Query(QueryLanguage),
    QueryWithFormat(QueryWithResponseFormat),
}
