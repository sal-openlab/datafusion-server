// custom_extractor.rs

use axum::{
    async_trait, body,
    extract::{FromRequest, Request},
    http::StatusCode,
    response::Response,
};

pub struct BodyWithLimit {
    #[allow(dead_code)]
    pub payload: bytes::Bytes,
}

#[async_trait]
impl<S> FromRequest<S> for BodyWithLimit
where
    bytes::Bytes: FromRequest<S>,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let max_size = 20 * 1024 * 1024; // 20 MB

        let bytes = match body::to_bytes(req.into_body(), max_size).await {
            Ok(bytes) => bytes,
            Err(_e) => {
                return Err(Response::builder()
                    .status(StatusCode::PAYLOAD_TOO_LARGE)
                    .body(body::Body::empty())
                    .unwrap());
            }
        };

        log::debug!("payload length: {} bytes", bytes.len());

        Ok(Self { payload: bytes })
    }
}
