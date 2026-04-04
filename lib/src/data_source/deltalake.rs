// data_source/deltalake.rs
// Sasaki, Naoki <nsasaki@sal.co.jp> June 15, 2024
//

use std::{io::Cursor, sync::Arc};

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader as Arrow58StreamReader;
use arrow_57::{
    array::RecordBatch as RecordBatch57, ipc::writer::StreamWriter as Arrow57StreamWriter,
};
use delta_kernel::{
    engine::{
        arrow_data::ArrowEngineData,
        default::{executor::tokio::TokioBackgroundExecutor, DefaultEngineBuilder},
    },
    scan::ScanBuilder,
    snapshot::Snapshot,
    DeltaResult,
};
use itertools::Itertools;
use object_store_012::{local::LocalFileSystem, DynObjectStore};
use url::Url;

use crate::data_source::location::{
    file,
    uri::{self, SupportedScheme},
};
use crate::response::http_error::ResponseError;

pub fn to_record_batch(
    uri: &str,
    _options: &crate::request::body::DataSourceOption,
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

    let engine = Arc::new(
        DefaultEngineBuilder::new(object_store)
            .with_task_executor(Arc::new(TokioBackgroundExecutor::new()))
            .build(),
    );

    let snapshot = Snapshot::builder_for(parsed_url)
        .build(engine.as_ref())
        .map_err(|e| ResponseError::request_validation(e.to_string()))?;

    let scan = ScanBuilder::new(snapshot).build()?;

    let batches57: Vec<RecordBatch57> = scan
        .execute(engine.clone())?
        .map(|scan_result| -> DeltaResult<_> {
            let data = scan_result?;
            let arrow_data = ArrowEngineData::try_from_engine_data(data)?;
            Ok(arrow_data.record_batch().clone())
        })
        .try_collect()?;

    batches57
        .iter()
        .map(convert_record_batch_57_to_58)
        .collect()
}

// TODO: temporary code for the delta_kernel_rs v0.20
fn convert_record_batch_57_to_58(batch57: &RecordBatch57) -> Result<RecordBatch, ResponseError> {
    let schema57 = batch57.schema();

    let mut buf = Vec::new();
    {
        let mut writer = Arrow57StreamWriter::try_new(&mut buf, &schema57).map_err(|e| {
            ResponseError::request_validation(format!("arrow ipc writer init failed: {e}"))
        })?;
        writer.write(batch57).map_err(|e| {
            ResponseError::request_validation(format!("arrow ipc write failed: {e}"))
        })?;
        writer.finish().map_err(|e| {
            ResponseError::request_validation(format!("arrow ipc finish failed: {e}"))
        })?;
    }

    let cursor = Cursor::new(buf);
    let mut reader = Arrow58StreamReader::try_new(cursor, None).map_err(|e| {
        ResponseError::request_validation(format!("arrow ipc reader init failed: {e}"))
    })?;

    match reader.next() {
        Some(Ok(batch58)) => Ok(batch58),
        Some(Err(e)) => Err(ResponseError::request_validation(format!(
            "arrow ipc read failed: {e}"
        ))),
        None => Err(ResponseError::request_validation(
            "arrow ipc read returned no record batch".to_string(),
        )),
    }
}

// TODO: temporary code for the delta_kernel_rs v0.20
fn build_store(
    scheme: &SupportedScheme,
    _authority: &str,
) -> Result<Arc<DynObjectStore>, ResponseError> {
    Err(ResponseError::request_validation(format!(
        "Delta Lake external object store is not supported yet for scheme '{scheme:?}' because delta_kernel uses object_store 0.12.x while this server uses object_store 0.13.x"
    )))
}

// fn build_store(
//     scheme: &SupportedScheme,
//     authority: &str,
// ) -> Result<Arc<DynObjectStore>, ResponseError> {
//     let key = ObjectStoreManager::store_key(scheme, authority);
//
//     if let Some(store) = Settings::global().object_store_manager.stores.get(&key) {
//         Ok(store.clone())
//     } else {
//         Err(ResponseError::request_validation(format!(
//             "Object store '{key}' credential not configured"
//         )))
//     }
// }
