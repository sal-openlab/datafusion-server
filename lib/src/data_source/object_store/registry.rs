// object_store/registry.rs

use std::env;
use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use object_store::{aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder};

use crate::response::http_error::ResponseError;
use crate::settings::{Settings, Storage};

pub fn register(ctx: &SessionContext) -> Result<(), ResponseError> {
    log::debug!("Register to object store from configuration");

    if let Some(storages) = &Settings::global().storages {
        for storage in storages {
            match storage {
                Storage::Aws(aws) => {
                    log::debug!("Register 's3://{}' to object store registry", &aws.bucket);

                    let s3 = AmazonS3Builder::new()
                        .with_bucket_name(&aws.bucket)
                        .with_region(&aws.region)
                        .with_access_key_id(&aws.access_key_id)
                        .with_secret_access_key(&aws.secret_access_key)
                        .build()?;

                    ctx.runtime_env().register_object_store(
                        &url::Url::parse(&format!("s3://{}", &aws.bucket))?,
                        Arc::new(s3),
                    );
                }
                Storage::Gcs(gcs) => {
                    log::debug!("Register 'gs://{}' to object store registry", &gcs.bucket);

                    let gs = GoogleCloudStorageBuilder::new()
                        .with_bucket_name(&gcs.bucket)
                        .with_service_account_key(&gcs.service_account_key)
                        .build()?;

                    ctx.runtime_env().register_object_store(
                        &url::Url::parse(&format!("gs://{}", &gcs.bucket))?,
                        Arc::new(gs),
                    );
                }
            }
        }
    }

    log::debug!("Register to object store from environment");

    if env::var("AWS_ACCESS_KEY_ID").is_ok() && env::var("AWS_BUCKET").is_ok() {
        let bucket = env::var("AWS_BUCKET").unwrap_or_default();

        log::debug!("Register 's3://{bucket}' to object store registry");

        let s3 = AmazonS3Builder::from_env()
            .with_bucket_name(&bucket)
            .build()?;

        ctx.runtime_env().register_object_store(
            &url::Url::parse(&format!("s3://{}", &bucket))?,
            Arc::new(s3),
        );
    }

    if env::var("GOOGLE_SERVICE_ACCOUNT_KEY").is_ok() && env::var("GOOGLE_BUCKET").is_ok() {
        let bucket = env::var("GOOGLE_BUCKET").unwrap_or_default();

        log::debug!("Register 'gs://{bucket}' to object store registry");

        let gs = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(&bucket)
            .build()?;

        ctx.runtime_env().register_object_store(
            &url::Url::parse(&format!("gs://{}", &bucket))?,
            Arc::new(gs),
        );
    }

    Ok(())
}
