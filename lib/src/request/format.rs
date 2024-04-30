// format.rs

use std::path::Path;
use std::str::FromStr;

use crate::request::body::DataSourceFormat;

pub fn resolve_from(
    content_type: Option<&str>,
    file_name: Option<&str>,
) -> Option<DataSourceFormat> {
    let format = if let Some(content_type) = content_type {
        if let Ok(content_type) = mime::Mime::from_str(content_type) {
            match (content_type.type_(), content_type.subtype().as_str()) {
                (mime::TEXT, "csv") => Some(DataSourceFormat::Csv),
                (mime::APPLICATION, "json") => Some(DataSourceFormat::Json),
                (mime::APPLICATION, "vnd.apache.parquet") => Some(DataSourceFormat::Parquet),
                #[cfg(feature = "avro")]
                (mime::APPLICATION, "vnd.apache.avro") => Some(DataSourceFormat::Avro),
                _ => None,
            }
        } else {
            None
        }
    } else {
        None
    };

    if format.is_none() {
        if let Some(file_name) = file_name {
            if let Some(extension) = Path::new(file_name).extension().and_then(|s| s.to_str()) {
                match &*String::from_str(extension)
                    .unwrap_or(String::new())
                    .to_lowercase()
                {
                    "csv" => Some(DataSourceFormat::Csv),
                    "json" => Some(DataSourceFormat::Json),
                    "parquet" => Some(DataSourceFormat::Parquet),
                    #[cfg(feature = "avro")]
                    "avro" => Some(DataSourceFormat::Avro),
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
        }
    } else {
        format
    }
}
