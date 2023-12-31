// context/location_uri - Location of data sources
// Sasaki, Naoki <nsasaki@sal.co.jp> January 3, 2023
//

use crate::response::http_error::ResponseError;
use axum::http::uri::{InvalidUri, Parts, Uri};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum InvalidLocation {
    #[error("Incorrect URI Format")]
    IncorrectUriFormat(#[from] InvalidUri),

    #[cfg(not(feature = "plugin"))]
    #[error("Unsupported Scheme")]
    UnsupportedScheme,
}

#[derive(Debug, PartialEq)]
pub enum SupportedScheme {
    Http,
    Https,
    File,
    #[cfg(feature = "plugin")]
    WillPlugin,
}

#[allow(clippy::unnecessary_wraps)] // never returns `Err` while enabling plugin feature
pub fn scheme(parts: &Parts) -> anyhow::Result<SupportedScheme> {
    if parts.scheme.is_none() {
        return Ok(SupportedScheme::File);
    }

    let scheme = &parts.scheme;
    match &*scheme.as_ref().unwrap().to_string() {
        "http" => Ok(SupportedScheme::Http),
        "https" => Ok(SupportedScheme::Https),
        "file" => Ok(SupportedScheme::File),
        #[cfg(feature = "plugin")]
        _ => Ok(SupportedScheme::WillPlugin),
        #[cfg(not(feature = "plugin"))]
        _ => Err(InvalidLocation::UnsupportedScheme.into()),
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
        .replacen("file:///", "file://_/", 1)
        .parse::<Uri>()?
        .into_parts();
    Ok(parts)
}

#[allow(dead_code)] // now only use for plugin feature
pub fn to_map(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::<String, String>::new();
    if !query.is_empty() {
        for kv in query.to_string().split('&') {
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
    use crate::data_source::location_uri;

    #[test]
    fn valid_full_qualified_uri() {
        let s = "http://authority:8080/path/foo";
        let uri = location_uri::to_parts(s).unwrap();
        assert_eq!(uri.authority.unwrap().as_str(), "authority:8080");
        assert_eq!(uri.path_and_query.unwrap().as_str(), "/path/foo");
    }

    #[test]
    fn valid_full_qualified_uri_with_query() {
        let s = "http://authority:8080/path?foo=bar&baz";
        let uri = location_uri::to_parts(s).unwrap();
        let pq = uri.path_and_query.unwrap();
        assert_eq!(pq.path(), "/path");
        assert_eq!(pq.query().unwrap(), "foo=bar&baz");
        let q = location_uri::to_map(pq.query().unwrap());
        assert_eq!(q.get("foo").unwrap(), "bar");
        assert_eq!(q.get("baz").unwrap(), "true");
    }

    #[test]
    fn valid_full_qualified_uri_without_query() {
        let s = "http://authority:8080/path/foo";
        let uri = location_uri::to_parts(s).unwrap();
        let pq = uri.path_and_query.unwrap();
        assert_eq!(pq.query(), None);
        let q = location_uri::to_map(pq.query().unwrap_or(""));
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn valid_file_uri() {
        let s = "file:///file.json";
        let uri = location_uri::to_parts(s).unwrap();
        let method = location_uri::scheme(&uri).unwrap();
        assert_eq!(method, location_uri::SupportedScheme::File);
        assert_eq!(uri.path_and_query.unwrap().as_str(), "/file.json");
    }

    #[test]
    fn valid_file_with_path_uri() {
        let s = "file:///path/file.json";
        let uri = location_uri::to_file_path_and_name(s).unwrap();
        assert_eq!(uri.as_str(), "path/file.json");
    }

    #[test]
    fn valid_method_with_file_only() {
        let s = "file:///file.json";
        let uri = location_uri::to_file_path_and_name(s).unwrap();
        assert_eq!(uri.as_str(), "file.json");
    }

    #[test]
    fn valid_file_only_uri() {
        let s = "/filename.json"; // can be omitted `file://` in this case
        let uri = location_uri::to_parts(s).unwrap();
        let method = location_uri::scheme(&uri).unwrap();
        assert_eq!(method, location_uri::SupportedScheme::File);
        assert_eq!(uri.path_and_query.unwrap().as_str(), "/filename.json");
    }
}
