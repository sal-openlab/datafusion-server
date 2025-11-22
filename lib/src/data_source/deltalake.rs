// data_source/deltalake.rs
// Sasaki, Naoki <nsasaki@sal.co.jp> June 15, 2024
//

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use delta_kernel::{
    engine::{
        arrow_data::ArrowEngineData,
        default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
    },
    scan::ScanBuilder,
    snapshot::Snapshot,
    DeltaResult,
};
use itertools::Itertools;
use object_store::{local::LocalFileSystem, DynObjectStore};
use url::Url;

use crate::data_source::location::{
    file,
    uri::{self, SupportedScheme},
};
use crate::data_source::object_store::credential_manager::ObjectStoreManager;
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;
use crate::settings::Settings;

pub fn to_record_batch(
    uri: &str,
    _options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    log::debug!("deltalake::to_record_batch(): uri={uri:?}");

    let parts = uri::to_parts(uri)?;
    let scheme = uri::scheme(&parts).unwrap_or(SupportedScheme::File);

    let location = format!(
        "{}/",
        if scheme == SupportedScheme::File {
            file::create_data_file_path(uri)?
        } else {
            uri.to_string()
        }
        .trim_end_matches('/')
    );
    let parsed_url = Url::parse(&location)
        .map_err(|e| ResponseError::request_validation(format!("invalid delta table url: {e}")))?;

    let object_store: Arc<DynObjectStore> = if scheme == SupportedScheme::File {
        Arc::new(LocalFileSystem::new()) as Arc<DynObjectStore>
    } else {
        build_store(
            &scheme,
            parts.authority.as_ref().map_or("", |auth| auth.as_str()),
        )?
    };

    let engine = Arc::new(DefaultEngine::new_with_executor(
        object_store,
        Arc::new(TokioBackgroundExecutor::new()),
    ));

    let snapshot = Snapshot::builder_for(parsed_url)
        .build(engine.as_ref())
        .map_err(|e| ResponseError::request_validation(e.to_string()))?;
    let scan = ScanBuilder::new(snapshot).build()?;

    let batches: Vec<RecordBatch> = scan
        .execute(engine.clone())?
        .map(|scan_result| -> DeltaResult<_> {
            let data = scan_result?;
            let record_batch: RecordBatch = data
                .into_any()
                .downcast::<ArrowEngineData>()
                .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
                .into();
            Ok(record_batch)
        })
        .try_collect()?;

    Ok(batches)
}

fn build_store(
    scheme: &SupportedScheme,
    authority: &str,
) -> Result<Arc<DynObjectStore>, ResponseError> {
    let key = ObjectStoreManager::store_key(scheme, authority);

    if let Some(store) = Settings::global().object_store_manager.stores.get(&key) {
        Ok(store.clone())
    } else {
        Err(ResponseError::request_validation(format!(
            "Object store '{key}' credential not configured"
        )))
    }
}
