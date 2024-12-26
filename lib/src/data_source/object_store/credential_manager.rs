// object_store/credential_manager.rs
// Sasaki, Naoki <nsasaki@sal.co.jp> June 17, 2024
//

use std::collections::{hash_map::Entry, HashMap};
use std::env;
use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::DynObjectStore;

use crate::data_source::location::uri::SupportedScheme;
#[cfg(feature = "webdav")]
use crate::data_source::object_store::build_store;
use crate::settings::Storage;

#[derive(Clone, Debug, Default)]
pub struct ObjectStoreManager {
    pub stores: HashMap<String, Arc<DynObjectStore>>,
}

impl ObjectStoreManager {
    pub fn new_with_config(
        storage_settings: Option<&Vec<Storage>>,
    ) -> Result<Self, object_store::Error> {
        let mut stores: HashMap<String, Arc<DynObjectStore>> = HashMap::new();

        Self::from_env(&mut stores)?;

        if let Some(storages) = storage_settings {
            Self::from_config(&mut stores, storages)?;
        }

        Ok(Self { stores })
    }

    fn from_config(
        stores: &mut HashMap<String, Arc<DynObjectStore>>,
        storages: &Vec<Storage>,
    ) -> Result<(), object_store::Error> {
        for storage in storages {
            match storage {
                Storage::Aws(aws) => {
                    let key = Self::store_key(&SupportedScheme::S3, &aws.bucket);
                    log::debug!("Create '{key}' object store registry");

                    if let Entry::Vacant(entry) = stores.entry(key.clone()) {
                        entry.insert(Arc::new(
                            AmazonS3Builder::new()
                                .with_bucket_name(&aws.bucket)
                                .with_region(&aws.region)
                                .with_access_key_id(&aws.access_key_id)
                                .with_secret_access_key(&aws.secret_access_key)
                                .build()?,
                        ));
                    } else {
                        log::error!("Duplicated credential '{key}'");
                    }
                }
                Storage::Gcp(gcp) => {
                    let key = Self::store_key(&SupportedScheme::GS, &gcp.bucket);
                    log::debug!("Create '{key}' object store registry");

                    if let Entry::Vacant(entry) = stores.entry(key.clone()) {
                        entry.insert(Arc::new(
                            GoogleCloudStorageBuilder::new()
                                .with_bucket_name(&gcp.bucket)
                                .with_service_account_key(&gcp.service_account_key)
                                .build()?,
                        ));
                    } else {
                        log::error!("Duplicated credential '{key}'");
                    }
                }
                Storage::Azure(azure) => {
                    let key = Self::store_key(&SupportedScheme::GS, &azure.container);
                    log::debug!("Create '{key}' object store registry");

                    if let Entry::Vacant(entry) = stores.entry(key.clone()) {
                        entry.insert(Arc::new(
                            MicrosoftAzureBuilder::new()
                                .with_account(&azure.account_name)
                                .with_access_key(&azure.access_key)
                                .with_container_name(&azure.container)
                                .build()?,
                        ));
                    } else {
                        log::error!("Duplicated credential '{key}'");
                    }
                }
                #[cfg(feature = "webdav")]
                Storage::Webdav(http) => {
                    log::debug!("Create '{}' object store registry", &http.url);

                    let (store, scheme, authority) = build_store::webdav(
                        &http.url,
                        http.user.as_ref().unwrap_or(&String::new()),
                        http.password.as_ref().unwrap_or(&String::new()),
                    )
                    .map_err(|e| object_store::Error::Generic {
                        store: "http",
                        source: Box::new(e),
                    })?;

                    let key = Self::store_key(&SupportedScheme::from_str(&scheme), &authority);

                    if let Entry::Vacant(entry) = stores.entry(key.clone()) {
                        entry.insert(store);
                    } else {
                        log::error!("Duplicated credential '{key}'");
                    }
                }
            }
        }

        Ok(())
    }

    fn from_env(
        stores: &mut HashMap<String, Arc<DynObjectStore>>,
    ) -> Result<(), object_store::Error> {
        if env::var("AWS_ACCESS_KEY_ID").is_ok()
            && env::var("AWS_SECRET_ACCESS_KEY").is_ok()
            && env::var("AWS_BUCKET").is_ok()
        {
            let bucket = env::var("AWS_BUCKET").unwrap_or_default();
            let key = Self::store_key(&SupportedScheme::S3, &bucket);

            log::debug!("Create '{key}' object store registry");

            stores.insert(
                key,
                Arc::new(
                    AmazonS3Builder::from_env()
                        .with_bucket_name(&bucket)
                        .build()?,
                ),
            );
        }

        if env::var("GOOGLE_SERVICE_ACCOUNT_KEY").is_ok() && env::var("GOOGLE_BUCKET").is_ok() {
            let bucket = env::var("GOOGLE_BUCKET").unwrap_or_default();
            let key = Self::store_key(&SupportedScheme::GS, &bucket);

            log::debug!("Create '{key}' object store registry");

            stores.insert(
                key,
                Arc::new(
                    GoogleCloudStorageBuilder::from_env()
                        .with_bucket_name(&bucket)
                        .build()?,
                ),
            );
        }

        if env::var("AZURE_STORAGE_ACCOUNT_NAME").is_ok()
            && env::var("AZURE_STORAGE_ACCESS_KEY").is_ok()
            && env::var("AZURE_CONTAINER").is_ok()
        {
            let container = env::var("AZURE_CONTAINER").unwrap_or_default();
            let key = Self::store_key(&SupportedScheme::AZ, &container);

            log::debug!("Create '{key}' object store registry");

            stores.insert(
                key,
                Arc::new(
                    MicrosoftAzureBuilder::from_env()
                        .with_container_name(&container)
                        .build()?,
                ),
            );
        }

        #[cfg(feature = "webdav")]
        if env::var("HTTP_URL").is_ok() {
            let url = env::var("HTTP_URL").unwrap_or_default();
            let user = env::var("HTTP_USER").unwrap_or_default();
            let password = env::var("HTTP_PASSWORD").unwrap_or_default();

            log::debug!("Create '{url}' object store registry");

            let (store, scheme, authority) =
                build_store::webdav(&url, &user, &password).map_err(|e| {
                    object_store::Error::Generic {
                        store: "http",
                        source: Box::new(e),
                    }
                })?;

            stores.insert(
                Self::store_key(&SupportedScheme::from_str(&scheme), &authority),
                store,
            );
        }

        Ok(())
    }

    pub fn store_key(scheme: &SupportedScheme, bucket: &str) -> String {
        format!("{}://{}", scheme.to_str(), bucket)
    }
}
