// data_source/location/uri.rs - Location of data sources
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use std::collections::HashMap;
use std::string::ToString;

use axum::http::uri::{InvalidUri, Parts, Uri};
use thiserror::Error;

#[cfg(feature = "plugin")]
use crate::plugin::plugin_manager::PluginManager;
use crate::response::http_error::ResponseError;
#[cfg(feature = "webdav")]
use crate::settings::{Settings, Storage};

#[derive(Error, Debug)]
pub enum InvalidLocation {
    #[error("Incorrect URI format")]
    IncorrectUriFormat(#[from] InvalidUri),
    #[error("Unsupported scheme")]
    UnsupportedScheme,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SupportedScheme {
    Http,
    Https,
    File,
    S3,
    GS,
    AZ,
    #[cfg(feature = "webdav")]
    Webdav,
    #[cfg(feature = "flight")]
    Grpc,
    #[cfg(feature = "flight")]
    GrpcTls,
    #[cfg(feature = "plugin")]
    Plugin,
}

impl SupportedScheme {
    #[allow(dead_code)]
    pub fn to_str(&self) -> &str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
            Self::File => "file",
            Self::S3 => "s3",
            Self::GS => "gs",
            Self::AZ => "az",
            #[cfg(feature = "webdav")]
            Self::Webdav => "webdav",
            #[cfg(feature = "flight")]
            Self::Grpc => "grpc",
            #[cfg(feature = "flight")]
            Self::GrpcTls => "grpc+tls",
            #[cfg(feature = "plugin")]
            Self::Plugin => "plugin",
        }
    }

    #[allow(dead_code)]
    pub fn from_str(scheme: &str) -> Self {
        match scheme {
            "http" => SupportedScheme::Http,
            "https" => SupportedScheme::Https,
            "file" => SupportedScheme::File,
            "s3" => SupportedScheme::S3,
            "gs" => SupportedScheme::GS,
            "az" | "adl" | "abfs" | "abfss" => SupportedScheme::AZ,
            #[cfg(feature = "webdav")]
            "webdav" => SupportedScheme::Webdav,
            #[cfg(feature = "flight")]
            "grpc" => SupportedScheme::Grpc,
            #[cfg(feature = "flight")]
            "grpc+tls" => SupportedScheme::GrpcTls,
            #[cfg(feature = "plugin")]
            "plugin" => SupportedScheme::Plugin,
            #[cfg(feature = "plugin")]
            _ => SupportedScheme::Plugin,
            #[cfg(not(feature = "plugin"))]
            _ => SupportedScheme::File,
        }
    }

    pub fn handle_object_store(&self) -> bool {
        match self {
            Self::File => true,
            Self::S3 => true,
            Self::GS => true,
            Self::AZ => true,
            #[cfg(feature = "webdav")]
            Self::Webdav => true,
            _ => false,
        }
    }

    pub fn remote_source(&self) -> bool {
        match self {
            Self::Http | Self::Https => true,
            #[cfg(feature = "flight")]
            Self::Grpc | Self::GrpcTls => true,
            _ => false,
        }
    }
}

pub fn scheme(parts: &Parts) -> anyhow::Result<SupportedScheme> {
    if let Some(scheme) = &parts.scheme {
        match &*scheme.to_string() {
            #[cfg(feature = "webdav")]
            "http" | "https" => {
                if let Some(storages) = &Settings::global().storages {
                    for storage in storages {
                        match storage {
                            Storage::Webdav(http) => {
                                let (scheme, authority, pq) = parts_to_string(parts);
                                if format!("{scheme}://{authority}{pq}").starts_with(&http.url) {
                                    return Ok(SupportedScheme::Webdav);
                                }
                            }
                            _ => continue,
                        }
                    }
                }

                match &*scheme.to_string() {
                    "http" => Ok(SupportedScheme::Http),
                    _ => Ok(SupportedScheme::Https),
                }
            }
            #[cfg(not(feature = "webdav"))]
            "http" => Ok(SupportedScheme::Http),
            #[cfg(not(feature = "webdav"))]
            "https" => Ok(SupportedScheme::Https),
            "file" => Ok(SupportedScheme::File),
            "s3" => Ok(SupportedScheme::S3),
            "gs" => Ok(SupportedScheme::GS),
            "az" | "adl" | "abfs" | "abfss" => Ok(SupportedScheme::AZ),
            #[cfg(feature = "flight")]
            "grpc" => Ok(SupportedScheme::Grpc),
            #[cfg(feature = "flight")]
            "grpc+tls" => Ok(SupportedScheme::GrpcTls),
            #[cfg(feature = "plugin")]
            _ => {
                if PluginManager::global()
                    .registered_schemes()
                    .contains(&scheme.to_string())
                {
                    Ok(SupportedScheme::Plugin)
                } else {
                    Err(InvalidLocation::UnsupportedScheme.into())
                }
            }
            #[cfg(not(feature = "plugin"))]
            _ => Err(InvalidLocation::UnsupportedScheme.into()),
        }
    } else {
        Ok(SupportedScheme::File)
    }
}

pub fn to_file_path_and_name(uri: &str) -> anyhow::Result<String> {
    let path_and_name = to_parts(uri)?
        .path_and_query
        .ok_or_else(|| ResponseError::unsupported_type("Not found file name in location URI"))?
        .to_string();

    Ok(if path_and_name.starts_with('/') {
        path_and_name.replacen('/', "", 1)
    } else {
        path_and_name
    })
}

pub fn to_parts(uri: &str) -> anyhow::Result<Parts> {
    let parts = uri
        .replacen("file:///", "file://./", 1)
        .parse::<Uri>()?
        .into_parts();
    Ok(parts)
}

#[cfg(feature = "webdav")]
pub fn parts_to_string(parts: &Parts) -> (String, String, String) {
    let scheme = parts
        .scheme
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();

    let authority = parts
        .authority
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();

    let pq = parts
        .path_and_query
        .as_ref()
        .map_or_else(|| "/".to_string(), ToString::to_string);

    (scheme, authority, pq)
}

#[allow(dead_code)] // now only use for plugin feature
pub fn to_map(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::<String, String>::new();
    if !query.is_empty() {
        for kv in query.split('&') {
            let kv = kv.to_string();
            let s: Vec<&str> = kv.split('=').collect();
            map.insert(
                s[0].to_string(),
                if s.len() > 1 {
                    s[1].to_string()
                } else {
                    "true".to_string()
                },
            );
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use crate::data_source::location;

    #[test]
    fn valid_full_qualified_uri() {
        let s = "http://authority:8080/path/foo";
        let uri = location::uri::to_parts(s).unwrap();
        assert_eq!(uri.authority.unwrap().as_str(), "authority:8080");
        assert_eq!(uri.path_and_query.unwrap().as_str(), "/path/foo");
    }

    #[test]
    fn valid_no_path_uri() {
        let s = "http://authority";
        let uri = location::uri::to_parts(s).unwrap();
        assert_eq!(uri.authority.unwrap().as_str(), "authority");
        assert_eq!(uri.path_and_query.unwrap().as_str(), "/");
    }

    #[test]
    fn valid_s3_bucket() {
        let s = "s3://bucket/path/to/file.csv";
        let uri = location::uri::to_parts(s).unwrap();
        assert_eq!(uri.authority.unwrap().as_str(), "bucket");
        assert_eq!(uri.path_and_query.unwrap().as_str(), "/path/to/file.csv");
    }

    #[test]
    fn valid_full_qualified_uri_with_query() {
        let s = "http://authority:8080/path?foo=bar&baz";
        let uri = location::uri::to_parts(s).unwrap();
        let pq = uri.path_and_query.unwrap();
        assert_eq!(pq.path(), "/path");
        assert_eq!(pq.query().unwrap(), "foo=bar&baz");
        let q = location::uri::to_map(pq.query().unwrap());
        assert_eq!(q.get("foo").unwrap(), "bar");
        assert_eq!(q.get("baz").unwrap(), "true");
    }

    #[test]
    fn valid_full_qualified_uri_without_query() {
        let s = "http://authority:8080/path/foo";
        let uri = location::uri::to_parts(s).unwrap();
        let pq = uri.path_and_query.unwrap();
        assert_eq!(pq.query(), None);
        let q = location::uri::to_map(pq.query().unwrap_or(""));
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn valid_file_uri() {
        let s = "file:///file.json";
        let uri = location::uri::to_parts(s).unwrap();
        let scheme = location::uri::scheme(&uri).unwrap();
        assert_eq!(scheme, location::uri::SupportedScheme::File);
        assert_eq!(uri.path_and_query.unwrap().as_str(), "/file.json");
    }

    #[test]
    fn valid_file_with_path_uri() {
        let s = "file:///path/file.json";
        let uri = location::uri::to_file_path_and_name(s).unwrap();
        assert_eq!(uri.as_str(), "path/file.json");
    }

    #[test]
    fn valid_method_with_file_only() {
        let s = "file:///file.json";
        let uri = location::uri::to_file_path_and_name(s).unwrap();
        assert_eq!(uri.as_str(), "file.json");
    }

    #[test]
    fn no_supported_relative_file_path() {
        let s = "file://file.json";
        let uri = location::uri::to_file_path_and_name(s).unwrap();
        assert_eq!(uri.as_str(), "");
    }

    #[test]
    fn valid_file_only_uri() {
        let s = "/file.json"; // can be omitted `file://` in this case
        let uri = location::uri::to_parts(s).unwrap();
        let scheme = location::uri::scheme(&uri).unwrap();
        assert_eq!(scheme, location::uri::SupportedScheme::File);
        assert_eq!(uri.path_and_query.unwrap().as_str(), "/file.json");
    }
}
