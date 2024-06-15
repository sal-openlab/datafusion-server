// data_source/deltalake.rs
// Sasaki, Naoki <nsasaki@sal.co.jp> June 15, 2024
//

use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use delta_kernel::{
    engine::{
        arrow_data::ArrowEngineData,
        default::{executor::tokio::TokioBackgroundExecutor, DefaultEngine},
    },
    scan::ScanBuilder,
    Engine, Table,
};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::DynObjectStore;

use crate::data_source::location::{
    file,
    uri::{self, SupportedScheme},
};
#[cfg(feature = "webdav")]
use crate::data_source::object_store::build_store;
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;
use crate::settings::{Settings, Storage};

pub async fn to_record_batch(
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

    let table = Table::try_from_uri(&location)
        .map_err(|e| ResponseError::request_validation(e.to_string()))?;

    let engine: Box<dyn Engine> = if scheme == SupportedScheme::File {
        let mut options: HashMap<&str, String> = HashMap::new();
        options.insert("skip_signature", "true".to_string()); // no cloud credentials are needed

        Box::new(DefaultEngine::try_new(
            table.location(),
            options,
            Arc::new(TokioBackgroundExecutor::new()),
        )?)
    } else {
        Box::new(DefaultEngine::new(
            build_store(
                scheme,
                parts.authority.as_ref().map_or("", |auth| auth.as_str()),
            )?,
            object_store::path::Path::from(
                parts.path_and_query.as_ref().map_or("/", |pq| pq.path()),
            ),
            Arc::new(TokioBackgroundExecutor::new()),
        ))
    };

    let snapshot = table.snapshot(engine.as_ref(), None)?;
    let scan = ScanBuilder::new(snapshot).build()?;

    let mut batches = vec![];

    for res in scan.execute(engine.as_ref())?.into_iter() {
        let record_batch: RecordBatch = res
            .raw_data?
            .into_any()
            .downcast::<ArrowEngineData>()
            .map_err(|_| delta_kernel::Error::EngineDataType("ArrowEngineData".to_string()))?
            .into();
        batches.push(record_batch);
    }

    Ok(batches)
}

fn build_store(
    scheme: SupportedScheme,
    authority: &str,
) -> Result<Arc<DynObjectStore>, ResponseError> {
    if let Some(storages) = &Settings::global().storages {
        for storage in storages {
            match storage {
                Storage::Aws(aws) => {
                    if scheme == SupportedScheme::S3 && aws.bucket == authority {
                        return Ok(Arc::new(
                            AmazonS3Builder::new()
                                .with_bucket_name(&aws.bucket)
                                .with_region(&aws.region)
                                .with_access_key_id(&aws.access_key_id)
                                .with_secret_access_key(&aws.secret_access_key)
                                .build()?,
                        ));
                    }
                }
                Storage::Gcp(gcp) => {
                    if scheme == SupportedScheme::GS && gcp.bucket == authority {
                        return Ok(Arc::new(
                            GoogleCloudStorageBuilder::new()
                                .with_bucket_name(&gcp.bucket)
                                .with_service_account_key(&gcp.service_account_key)
                                .build()?,
                        ));
                    }
                }
                Storage::Azure(azure) => {
                    if scheme == SupportedScheme::AZ && azure.container == authority {
                        return Ok(Arc::new(
                            MicrosoftAzureBuilder::new()
                                .with_account(&azure.account_name)
                                .with_access_key(&azure.access_key)
                                .with_container_name(&azure.container)
                                .build()?,
                        ));
                    }
                }
                #[cfg(feature = "webdav")]
                Storage::Webdav(http) => {
                    let parts = uri::to_parts(&http.url)?;
                    let config_authority =
                        parts.authority.as_ref().map_or("", |auth| auth.as_str());

                    if scheme == SupportedScheme::Webdav && config_authority == authority {
                        return Ok(Arc::new(build_store::webdav(
                            &http.url,
                            http.user.as_ref().unwrap_or(&String::new()),
                            http.password.as_ref().unwrap_or(&String::new()),
                        )?));
                    }
                }
            }
        }
    }

    if scheme == SupportedScheme::S3
        && env::var("AWS_ACCESS_KEY_ID").is_ok()
        && env::var("AWS_SECRET_ACCESS_KEY").is_ok()
        && env::var("AWS_REGION").is_ok()
        && env::var("AWS_BUCKET").unwrap_or_default() == authority
    {
        return Ok(Arc::new(
            AmazonS3Builder::new()
                .with_bucket_name(env::var("AWS_BUCKET").unwrap_or_default())
                .with_region(env::var("AWS_REGION").unwrap_or_default())
                .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap_or_default())
                .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default())
                .build()?,
        ));
    }

    if scheme == SupportedScheme::GS
        && env::var("GOOGLE_SERVICE_ACCOUNT_KEY").is_ok()
        && env::var("GOOGLE_BUCKET").unwrap_or_default() == authority
    {
        return Ok(Arc::new(
            GoogleCloudStorageBuilder::new()
                .with_bucket_name(env::var("GOOGLE_BUCKET").unwrap_or_default())
                .with_service_account_key(
                    env::var("GOOGLE_SERVICE_ACCOUNT_KEY").unwrap_or_default(),
                )
                .build()?,
        ));
    }

    if scheme == SupportedScheme::AZ
        && env::var("AZURE_STORAGE_ACCOUNT_NAME").is_ok()
        && env::var("AZURE_STORAGE_ACCESS_KEY").is_ok()
        && env::var("AZURE_CONTAINER").unwrap_or_default() == authority
    {
        return Ok(Arc::new(
            MicrosoftAzureBuilder::new()
                .with_account(env::var("AZURE_STORAGE_ACCOUNT_NAME").unwrap_or_default())
                .with_access_key(env::var("AZURE_STORAGE_ACCESS_KEY").unwrap_or_default())
                .with_container_name(env::var("AZURE_CONTAINER").unwrap_or_default())
                .build()?,
        ));
    }

    #[cfg(feature = "webdav")]
    if scheme == SupportedScheme::Webdav && env::var("HTTP_URL").is_ok() {
        let parts = uri::to_parts(&env::var("HTTP_URL").unwrap_or_default())?;
        let config_authority = parts.authority.as_ref().map_or("", |auth| auth.as_str());

        if scheme == SupportedScheme::Webdav && config_authority == authority {
            return Ok(Arc::new(build_store::webdav(
                &env::var("HTTP_URL").unwrap_or_default(),
                &env::var("HTTP_USER").unwrap_or_default(),
                &env::var("HTTP_PASSWORD").unwrap_or_default(),
            )?));
        }
    }

    Err(ResponseError::request_validation(format!(
        "Storage credentials has not configured '{}://{}'",
        scheme.to_string(),
        authority
    )))
}
