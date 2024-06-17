// object_store/build_store.rs
// Sasaki, Naoki <nsasaki@sal.co.jp> June 15, 2024
//

#[cfg(feature = "webdav")]
use std::sync::Arc;

#[cfg(feature = "webdav")]
use object_store::{http::HttpBuilder, ClientOptions, DynObjectStore};

#[cfg(feature = "webdav")]
use crate::data_source::location;
#[cfg(feature = "webdav")]
use crate::response::http_error::ResponseError;

#[cfg(feature = "webdav")]
pub fn webdav(
    url: &str,
    user: &str,
    password: &str,
) -> Result<(Arc<DynObjectStore>, String, String), ResponseError> {
    let parts = location::uri::to_parts(url)?;
    let (scheme, authority, _) = location::uri::parts_to_string(&parts);

    let url_with_auth = if !user.to_string().is_empty() || !password.to_string().is_empty() {
        format!("{scheme}://{user}:{password}@{authority}")
    } else {
        format!("{scheme}://{authority}")
    };

    let http_store = HttpBuilder::new()
        .with_url(url_with_auth)
        .with_client_options(ClientOptions::new().with_allow_http(true))
        .build()?;

    Ok((Arc::new(http_store), scheme, authority))
}
