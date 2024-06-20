// object_store/registry.rs

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use object_store::DynObjectStore;

use crate::response::http_error::ResponseError;
use crate::settings::Settings;

pub fn register(ctx: &SessionContext) -> Result<(), ResponseError> {
    log::debug!("Register to object store from credential manager");

    for (key, store) in &Settings::global().object_store_manager.stores {
        register_to_runtime_env(ctx, key, store.clone())?;
    }

    Ok(())
}

fn register_to_runtime_env(
    ctx: &SessionContext,
    url: &str,
    store: Arc<DynObjectStore>,
) -> Result<(), ResponseError> {
    ctx.runtime_env()
        .register_object_store(&url::Url::parse(url)?, store);

    Ok(())
}
