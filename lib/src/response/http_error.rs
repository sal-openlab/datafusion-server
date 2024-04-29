// http_error: Error response handler
// Sasaki, Naoki <nsasaki@sal.co.jp> January 1, 2023
//

use axum::http::{self, Response};
use datafusion::{arrow, error::DataFusionError, parquet};
use serde::Serializer;
use serde_derive::Serialize;

#[derive(Serialize, thiserror::Error, Debug)]
pub struct ResponseError {
    #[serde(serialize_with = "serialize_status_code")]
    pub code: http::StatusCode,
    pub error: String,
    pub message: String,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // reference required for serde::Serializer
fn serialize_status_code<S>(x: &http::StatusCode, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_u16(x.as_u16())
}

impl std::fmt::Display for ResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{}]({}): {}", self.code, self.error, self.message)
    }
}

impl From<std::io::Error> for ResponseError {
    fn from(e: std::io::Error) -> Self {
        ResponseError {
            error: "io_error".to_string(),
            message: e.to_string(),
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<arrow::error::ArrowError> for ResponseError {
    fn from(e: arrow::error::ArrowError) -> Self {
        ResponseError {
            error: "arrow_error".to_string(),
            message: e.to_string(),
            code: http::StatusCode::BAD_REQUEST,
        }
    }
}

impl From<DataFusionError> for ResponseError {
    fn from(e: DataFusionError) -> Self {
        ResponseError {
            error: "data_fusion_error".to_string(),
            message: e.to_string(),
            code: http::StatusCode::BAD_REQUEST,
        }
    }
}

impl From<reqwest::Error> for ResponseError {
    fn from(e: reqwest::Error) -> Self {
        ResponseError {
            error: "http_request_error".to_string(),
            message: e.to_string(),
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<anyhow::Error> for ResponseError {
    fn from(e: anyhow::Error) -> Self {
        ResponseError {
            error: "uncategorized_error".to_string(),
            message: e.to_string(),
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<http::Error> for ResponseError {
    fn from(e: http::Error) -> Self {
        ResponseError {
            error: "axum_http_error".to_string(),
            message: e.to_string(),
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl axum::response::IntoResponse for ResponseError {
    fn into_response(self) -> axum::response::Response {
        let payload = serde_json::to_string(&self).unwrap();
        let body = axum::body::Body::new(axum::body::Body::from(payload));

        Response::builder().status(self.code).body(body).unwrap()
    }
}

impl ResponseError {
    pub fn session_not_found(id: impl Into<String>) -> Self {
        Self {
            code: http::StatusCode::BAD_REQUEST,
            error: "session_not_found".to_string(),
            message: id.into(),
        }
    }

    pub fn payload_too_large(message: impl Into<String>) -> Self {
        Self {
            code: http::StatusCode::PAYLOAD_TOO_LARGE,
            error: "payload_too_large".to_string(),
            message: message.into(),
        }
    }

    pub fn unsupported_type(message: impl Into<String>) -> Self {
        Self {
            code: http::StatusCode::BAD_REQUEST,
            error: "unsupported_type".to_string(),
            message: message.into(),
        }
    }

    pub fn unsupported_format(message: impl Into<String>) -> Self {
        Self {
            code: http::StatusCode::BAD_REQUEST,
            error: "unsupported_format".to_string(),
            message: message.into(),
        }
    }

    pub fn request_validation(message: impl Into<String>) -> Self {
        Self {
            code: http::StatusCode::BAD_REQUEST,
            error: "request_validating".to_string(),
            message: message.into(),
        }
    }

    pub fn json_parsing(message: impl Into<String>) -> Self {
        Self {
            code: http::StatusCode::BAD_REQUEST,
            error: "json_parsing".to_string(),
            message: message.into(),
        }
    }

    pub fn record_batch_creation(_: arrow::error::ArrowError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "record_batch_creation".to_string(),
            message: "Failed to create data source into record batches".to_string(),
        }
    }

    pub fn record_batch_extraction(_: arrow::error::ArrowError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "record_batch_extraction".to_string(),
            message: "Failed to extract from record batches".to_string(),
        }
    }

    pub fn parquet_deserialization(_: parquet::errors::ParquetError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "parquet_deserialization".to_string(),
            message: "Failed to deserialize parquet file into record batches".to_string(),
        }
    }

    pub fn parquet_serialization(_: parquet::errors::ParquetError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "parquet_serialization".to_string(),
            message: "Failed to serialize record batches into parquet file".to_string(),
        }
    }

    pub fn arrow_stream_serialization(_: arrow::error::ArrowError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "arrow_stream_serialization".to_string(),
            message: "Failed to serialize record batches into arrow stream".to_string(),
        }
    }

    pub fn json_stream_serialization(_: arrow::error::ArrowError) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "json_stream_serialization".to_string(),
            message: "Failed to serialize record batches into JSON stream".to_string(),
        }
    }

    pub fn http_request(_: reqwest::Error) -> Self {
        Self {
            code: http::StatusCode::EXPECTATION_FAILED,
            error: "http_request".to_string(),
            message: "Failed to request external HTTP server".to_string(),
        }
    }

    pub fn already_existing(message: impl Into<String>) -> Self {
        Self {
            code: http::StatusCode::BAD_REQUEST,
            error: "already_existing".to_string(),
            message: message.into(),
        }
    }

    pub fn internal_server_error(message: impl Into<String>) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "internal_server_error".to_string(),
            message: message.into(),
        }
    }

    #[allow(dead_code)]
    pub fn connection_by_peer(message: impl Into<String>) -> Self {
        Self {
            code: http::StatusCode::EXPECTATION_FAILED,
            error: "peer_connection_error".to_string(),
            message: message.into(),
        }
    }

    #[cfg(feature = "plugin")]
    pub fn python_interpreter_error(message: impl Into<String>) -> Self {
        Self {
            code: http::StatusCode::INTERNAL_SERVER_ERROR,
            error: "python_interpreter_error".to_string(),
            message: message.into(),
        }
    }
}
