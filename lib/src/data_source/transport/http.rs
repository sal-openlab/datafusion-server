// transport/http.rs

use std::collections::HashMap;
use std::str::FromStr;

use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Client,
};

use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;

pub enum ResponseDataType {
    Text,
    Binary,
}

pub enum ResponseData {
    Text(String),
    Binary(bytes::Bytes),
}

pub async fn get(
    uri: &str,
    options: &DataSourceOption,
    data_type: ResponseDataType,
) -> Result<ResponseData, ResponseError> {
    let response = Client::new()
        .get(uri)
        .headers(to_header_map(options.headers.as_ref())?)
        .send()
        .await
        .map_err(ResponseError::http_request)?;

    Ok(match data_type {
        ResponseDataType::Text => ResponseData::Text(response.text().await?),
        ResponseDataType::Binary => ResponseData::Binary(response.bytes().await?),
    })
}

fn to_header_map(headers: Option<&HashMap<String, String>>) -> Result<HeaderMap, ResponseError> {
    let mut result = HeaderMap::new();

    if let Some(headers) = headers {
        for (key, value) in headers {
            result.append(
                HeaderName::from_str(key).map_err(|e| {
                    ResponseError::request_validation(format!(
                        "Invalid http request header name: {e}"
                    ))
                })?,
                HeaderValue::from_str(value).map_err(|e| {
                    ResponseError::request_validation(format!(
                        "Invalid http request header value: {e}"
                    ))
                })?,
            );
        }
    }

    Ok(result)
}
