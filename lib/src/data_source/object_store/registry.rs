// object_store/registry.rs

use std::env;
use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use object_store::{
    aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder,
};
#[cfg(feature = "webdav")]
use object_store::{http::HttpBuilder, ClientOptions};

#[cfg(feature = "webdav")]
use crate::data_source::location;
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
                Storage::Gcp(gcp) => {
                    log::debug!("Register 'gs://{}' to object store registry", &gcp.bucket);

                    let gs = GoogleCloudStorageBuilder::new()
                        .with_bucket_name(&gcp.bucket)
                        .with_service_account_key(&gcp.service_account_key)
                        .build()?;

                    ctx.runtime_env().register_object_store(
                        &url::Url::parse(&format!("gs://{}", &gcp.bucket))?,
                        Arc::new(gs),
                    );
                }
                Storage::Azure(azure) => {
                    log::debug!(
                        "Register 'az://{}' to object store registry",
                        &azure.container
                    );

                    let az = MicrosoftAzureBuilder::new()
                        .with_account(&azure.account_name)
                        .with_access_key(&azure.access_key)
                        .with_container_name(&azure.container)
                        .build()?;

                    ctx.runtime_env().register_object_store(
                        &url::Url::parse(&format!("az://{}", &azure.container))?,
                        Arc::new(az),
                    );
                }
                #[cfg(feature = "webdav")]
                Storage::Webdav(http) => {
                    log::debug!("Register '{}' to object store registry", &http.url);

                    register_webdav(
                        ctx,
                        &http.url,
                        http.user.as_ref().unwrap_or(&String::new()),
                        http.password.as_ref().unwrap_or(&String::new()),
                    )?;
                }
            }
        }
    }

    register_from_env(ctx)?;

    Ok(())
}

fn register_from_env(ctx: &SessionContext) -> Result<(), ResponseError> {
    log::debug!("Register to object store from environment");

    if env::var("AWS_ACCESS_KEY_ID").is_ok()
        && env::var("AWS_SECRET_ACCESS_KEY").is_ok()
        && env::var("AWS_BUCKET").is_ok()
    {
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

    if env::var("AZURE_STORAGE_ACCOUNT_NAME").is_ok()
        && env::var("AZURE_STORAGE_ACCESS_KEY").is_ok()
        && env::var("AZURE_CONTAINER").is_ok()
    {
        let container = env::var("AZURE_CONTAINER").unwrap_or_default();

        log::debug!("Register 'az://{container}' to object store registry");

        let az = MicrosoftAzureBuilder::from_env()
            .with_container_name(&container)
            .build()?;

        ctx.runtime_env().register_object_store(
            &url::Url::parse(&format!("az://{}", &container))?,
            Arc::new(az),
        );
    }

    #[cfg(feature = "webdav")]
    if env::var("HTTP_URL").is_ok() {
        let url = env::var("HTTP_URL").unwrap_or_default();
        let user = env::var("HTTP_USER").unwrap_or_default();
        let password = env::var("HTTP_PASSWORD").unwrap_or_default();

        log::debug!("Register '{url}' to object store registry");

        register_webdav(ctx, &url, &user, &password)?;
    }

    Ok(())
}

#[cfg(feature = "webdav")]
fn register_webdav(
    ctx: &SessionContext,
    url: &str,
    user: &str,
    password: &str,
) -> Result<(), ResponseError> {
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

    ctx.runtime_env()
        .register_object_store(&url::Url::parse(url)?, Arc::new(http_store));

    Ok(())
}
