// data_source/deltalake.rs
// Sasaki, Naoki <nsasaki@sal.co.jp> June 15, 2024
//

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::{array::RecordBatch, compute};
use delta_kernel::{
    engine::{
        arrow_data::ArrowEngineData,
        default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
    },
    scan::ScanBuilder,
    DeltaResult, Table,
};
use itertools::Itertools;
use object_store::DynObjectStore;

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
    options: &DataSourceOption,
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

    let table = Table::try_from_uri(location)
        .map_err(|e| ResponseError::request_validation(e.to_string()))?;

    let engine = Arc::new(if scheme == SupportedScheme::File {
        let mut options: HashMap<&str, String> = HashMap::new();
        options.insert("skip_signature", "true".to_string()); // no cloud credentials are needed

        DefaultEngine::try_new(
            table.location(),
            options,
            Arc::new(TokioBackgroundExecutor::new()),
        )?
    } else {
        DefaultEngine::new(
            build_store(
                &scheme,
                parts.authority.as_ref().map_or("", |auth| auth.as_str()),
            )?,
            object_store::path::Path::from(
                parts.path_and_query.as_ref().map_or("/", |pq| pq.path()),
            ),
            Arc::new(TokioBackgroundExecutor::new()),
        )
    });

    let snapshot = table.snapshot(engine.as_ref(), options.version)?;
    let scan = ScanBuilder::new(snapshot).build()?;

    let batches: Vec<RecordBatch> = scan
        .execute(engine)?
        .map(|scan_result| -> DeltaResult<_> {
            let scan_result = scan_result?;
            let mask = scan_result.full_mask();
            let data = scan_result.raw_data?;
            let record_batch: RecordBatch = data
                .into_any()
                .downcast::<ArrowEngineData>()
                .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
                .into();
            if let Some(mask) = mask {
                Ok(compute::filter_record_batch(&record_batch, &mask.into())?)
            } else {
                Ok(record_batch)
            }
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
