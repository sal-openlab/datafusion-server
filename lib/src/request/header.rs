// header.rs - Request header handler
// Sasaki, Naoki <nsasaki@sal.co.jp> February 21, 2024
//

use axum::http::header;
use axum_extra::headers::{self, Header, HeaderName, HeaderValue};
use axum_extra::TypedHeader;

use crate::request::body::ResponseFormat;
use crate::response::http_error::ResponseError;

#[derive(Debug)]
pub struct Accept(String);

impl Header for Accept {
    fn name() -> &'static HeaderName {
        &header::ACCEPT
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, headers::Error>
    where
        I: Iterator<Item = &'i HeaderValue>,
    {
        values
            .next()
            .and_then(|value| value.to_str().ok())
            .map(|value| Accept(value.to_string()))
            .ok_or_else(headers::Error::invalid)
    }

    fn encode<E: Extend<HeaderValue>>(&self, values: &mut E) {
        let value = HeaderValue::from_str(&self.0).expect("Invalid accept header");
        values.extend(std::iter::once(value));
    }
}

pub fn response_format(
    accept_header: &TypedHeader<Accept>,
) -> Result<ResponseFormat, ResponseError> {
    let mut values: Vec<HeaderValue> = Vec::new();
    accept_header.0.encode(&mut values);

    let media_type = values
        .first()
        .ok_or_else(|| ResponseError::request_validation("Accept header should have a value"))?
        .to_str()
        .map_err(|_| ResponseError::internal_server_error("Failed to access the Accept header"))?;

    match media_type {
        "application/json" => Ok(ResponseFormat::Json),
        "text/csv" => Ok(ResponseFormat::Csv),
        "application/vnd.apache.arrow.stream" => Ok(ResponseFormat::Arrow),
        _ => Err(ResponseError::unsupported_format(format!(
            "Unsupported response format '{media_type}'"
        ))),
    }
}
