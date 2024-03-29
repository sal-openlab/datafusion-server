// session.rs - Session context manager.
// Sasaki, Naoki <nsasaki@sal.co.jp> January 14, 2023
//

use crate::context::session::{ConcurrentSessionContext, Session, SessionContext};
use crate::data_source::{location_uri, location_uri::SupportedScheme, schema::DataSourceSchema};
use crate::request::body::{
    DataSource, DataSourceFormat, MergeDirection, MergeOption, MergeProcessor,
};
use crate::response::{handler, http_error::ResponseError};
use crate::PluginManager;
use axum::async_trait;
use axum::http::uri;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionConfig;
use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct SessionContextManager {
    contexts: Arc<RwLock<HashMap<String, ConcurrentSessionContext>>>,
}

impl SessionContextManager {
    pub fn new() -> Self {
        let contexts = Arc::new(RwLock::new(
            HashMap::<String, ConcurrentSessionContext>::new(),
        ));
        Self { contexts }
    }
}

#[async_trait]
pub trait SessionManager: Send + Sync + 'static {
    async fn create_new_session(
        &self,
        config: Option<SessionConfig>,
        keep_alive: Option<i64>,
    ) -> String;
    async fn destroy_session(&self, session_id: &str) -> Result<(), ResponseError>;
    async fn cleanup(&self);
    async fn session_ids(&self) -> Vec<String>;
    async fn session(&self, session_id: &str) -> Result<handler::session::Session, ResponseError>;
    async fn data_source_names(&self, session_id: &str) -> Result<Vec<String>, ResponseError>;
    async fn data_source(
        &self,
        session_id: &str,
        name: &str,
    ) -> Result<handler::data_source::DataSourceDetail, ResponseError>;

    async fn append_data_source(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError>;

    async fn append_data_sources<'a>(
        &self,
        session_id: &str,
        data_sources: &'a [DataSource],
    ) -> Result<(), ResponseError>;

    async fn save_data_source(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError>;

    async fn save_data_sources<'a>(
        &self,
        session_id: &str,
        data_sources: &'a [DataSource],
    ) -> Result<(), ResponseError>;

    async fn refresh_data_source(&self, session_id: &str, name: &str) -> Result<(), ResponseError>;
    async fn remove_data_source(&self, session_id: &str, name: &str) -> Result<(), ResponseError>;

    async fn append_csv_file(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError>;

    async fn append_json_file(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError>;

    async fn append_json_rest(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError>;

    #[cfg(feature = "plugin")]
    async fn append_connector_plugin(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError>;

    async fn append_parquet(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError>;

    async fn execute_merge_processor(
        &self,
        session_id: &str,
        merge_processor: &MergeProcessor,
    ) -> Result<(), ResponseError>;

    async fn execute_merge_processors<'a>(
        &self,
        session_id: &str,
        merge_processors: &'a [MergeProcessor],
    ) -> Result<(), ResponseError>;

    async fn execute_sql(
        &self,
        session_id: &str,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, ResponseError>;
}

macro_rules! context {
    ($self:expr, $session_id:expr) => {
        $self
            .contexts
            .read()
            .await
            .get($session_id)
            .ok_or_else(|| ResponseError::session_not_found($session_id))
    };
}

#[async_trait]
impl SessionManager for SessionContextManager {
    async fn create_new_session(
        &self,
        config: Option<SessionConfig>,
        keep_alive: Option<i64>,
    ) -> String {
        let context = match config {
            Some(config) => {
                ConcurrentSessionContext::new(SessionContext::new_with_config(config, keep_alive))
            }
            None => ConcurrentSessionContext::new(SessionContext::new(keep_alive)),
        };

        let session_id = context.id().await;
        let mut contexts = self.contexts.write().await;
        contexts.insert(session_id.clone(), context);

        session_id
    }

    async fn destroy_session(&self, session_id: &str) -> Result<(), ResponseError> {
        if !(self.contexts.read().await).contains_key(session_id) {
            return Err(ResponseError::session_not_found(session_id));
        }

        (self.contexts.write().await).remove(session_id);
        Ok(())
    }

    async fn cleanup(&self) {
        let mut expired_ids: Vec<String> = vec![];

        for session_id in self.session_ids().await {
            if let Some(context) = (self.contexts.read().await).get(&session_id) {
                if context.expired().await {
                    expired_ids.push(session_id.clone());
                }
            }
        }

        for session_id in expired_ids {
            log::info!("Session {} has been expired", session_id);
            (self.contexts.write().await).remove(&session_id);
        }
    }

    async fn session_ids(&self) -> Vec<String> {
        (self.contexts.read().await)
            .keys()
            .cloned()
            .collect::<Vec<String>>()
    }

    async fn session(&self, session_id: &str) -> Result<handler::session::Session, ResponseError> {
        match (self.contexts.read().await).get(session_id) {
            Some(context) => Ok(handler::session::Session {
                id: context.id().await.clone(),
                created: context
                    .session_start_time()
                    .await
                    .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                ttl: cmp::max(context.ttl().await, 0),
            }),
            None => Err(ResponseError::session_not_found(session_id)),
        }
    }

    async fn data_source_names(&self, session_id: &str) -> Result<Vec<String>, ResponseError> {
        match (self.contexts.read().await).get(session_id) {
            Some(context) => Ok(context.data_source_names().await),
            None => Err(ResponseError::session_not_found(session_id)),
        }
    }

    async fn data_source(
        &self,
        session_id: &str,
        name: &str,
    ) -> Result<handler::data_source::DataSourceDetail, ResponseError> {
        match (self.contexts.read().await).get(session_id) {
            Some(context) => Ok({
                let (data_source, schema) = context.data_source(name).await?;
                handler::data_source::DataSourceDetail {
                    name: name.to_string(),
                    location: if let Some(ds) = data_source {
                        Some(ds.location)
                    } else {
                        None
                    },
                    schema: DataSourceSchema::from_datafusion_schema(&schema),
                }
            }),
            None => Err(ResponseError::session_not_found(session_id)),
        }
    }

    async fn append_data_source(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError> {
        #[inline]
        fn external_source(scheme: &SupportedScheme) -> bool {
            *scheme != SupportedScheme::File
        }

        let plugin_source = |uri: &uri::Parts| -> bool {
            let plugin_schemes = PluginManager::global().registered_schemes();
            plugin_schemes.contains(&uri.scheme.as_ref().unwrap().to_string())
        };

        let uri = location_uri::to_parts(&data_source.location)
            .map_err(|e| ResponseError::unsupported_type(e.to_string()))?;
        let scheme = location_uri::scheme(&uri)?;

        data_source.validator()?;

        #[cfg(feature = "plugin")]
        if scheme == SupportedScheme::WillPlugin && !plugin_source(&uri) {
            use std::str::FromStr;
            return Err(ResponseError::request_validation(format!(
                "Unsupported scheme '{}'",
                &uri.scheme
                    .unwrap_or_else(|| uri::Scheme::from_str("unknown").unwrap())
            )));
        }

        if external_source(&scheme) && plugin_source(&uri) {
            #[cfg(feature = "plugin")]
            self.append_connector_plugin(session_id, data_source)
                .await?;
        } else {
            match data_source.format {
                DataSourceFormat::Csv => {
                    if !external_source(&scheme) {
                        self.append_csv_file(session_id, data_source).await?;
                    }
                }
                DataSourceFormat::Parquet => {
                    if !external_source(&scheme) {
                        self.append_parquet(session_id, data_source).await?;
                    }
                }
                DataSourceFormat::Json | DataSourceFormat::NdJson => {
                    if external_source(&scheme) {
                        self.append_json_rest(session_id, data_source).await?;
                    } else {
                        self.append_json_file(session_id, data_source).await?;
                    }
                }
                DataSourceFormat::Arrow => {
                    // MEMO: will not to be reached this control path
                    return Err(ResponseError::request_validation(
                        "Invalid data source scheme 'arrow', use 'csv', 'json', 'ndJson' and 'parquet'.",
                    ));
                }
            }
        }

        Ok(())
    }

    async fn append_data_sources<'a>(
        &self,
        session_id: &str,
        data_sources: &'a [DataSource],
    ) -> Result<(), ResponseError> {
        for data_source in data_sources {
            self.append_data_source(session_id, data_source).await?;
        }

        Ok(())
    }

    async fn save_data_source(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError> {
        let uri = location_uri::to_parts(&data_source.location)
            .map_err(|e| ResponseError::unsupported_type(e.to_string()))?;
        let scheme = location_uri::scheme(&uri)?;

        if scheme != SupportedScheme::File {
            use std::str::FromStr;
            return Err(ResponseError::request_validation(format!(
                "Unsupported scheme '{}' save feature currently supported only 'file'",
                &uri.scheme
                    .unwrap_or_else(|| uri::Scheme::from_str("unknown").unwrap())
            )));
        }

        context!(self, session_id)?
            .save_to_file(data_source)
            .await?;

        Ok(())
    }

    async fn save_data_sources<'a>(
        &self,
        session_id: &str,
        data_sources: &'a [DataSource],
    ) -> Result<(), ResponseError> {
        for data_source in data_sources {
            self.save_data_source(session_id, data_source).await?;
        }

        Ok(())
    }

    async fn refresh_data_source(&self, session_id: &str, name: &str) -> Result<(), ResponseError> {
        let (data_source, _schema) = context!(self, session_id)?.data_source(name).await?;

        if data_source.is_none() {
            return Err(ResponseError::request_validation(
                "Can only be refreshed registered data source",
            ));
        }

        context!(self, session_id)?.remove_data_source(name).await?;
        self.append_data_source(session_id, &data_source.unwrap())
            .await?;

        Ok(())
    }

    async fn remove_data_source(&self, session_id: &str, name: &str) -> Result<(), ResponseError> {
        context!(self, session_id)?.remove_data_source(name).await?;
        Ok(())
    }

    async fn append_csv_file(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError> {
        context!(self, session_id)?
            .append_from_csv_file(data_source)
            .await?;
        Ok(())
    }

    async fn append_json_file(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError> {
        context!(self, session_id)?
            .append_from_json_file(data_source)
            .await?;
        Ok(())
    }

    async fn append_json_rest(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError> {
        context!(self, session_id)?
            .append_from_json_rest(data_source)
            .await?;
        Ok(())
    }

    #[cfg(feature = "plugin")]
    async fn append_connector_plugin(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError> {
        context!(self, session_id)?
            .append_from_connector_plugin(data_source)
            .await?;
        Ok(())
    }

    async fn append_parquet(
        &self,
        session_id: &str,
        data_source: &DataSource,
    ) -> Result<(), ResponseError> {
        context!(self, session_id)?
            .append_from_parquet(data_source)
            .await?;
        Ok(())
    }

    async fn execute_merge_processor(
        &self,
        session_id: &str,
        merge_processor: &MergeProcessor,
    ) -> Result<(), ResponseError> {
        merge_processor.validator()?;
        context!(self, session_id)?
            .execute_merge_processor(merge_processor)
            .await?;
        Ok(())
    }

    async fn execute_merge_processors<'a>(
        &self,
        session_id: &str,
        merge_processors: &'a [MergeProcessor],
    ) -> Result<(), ResponseError> {
        for merge_processor in merge_processors {
            self.execute_merge_processor(session_id, merge_processor)
                .await?;
        }

        for merge_processor in merge_processors {
            let options = if let Some(options) = &merge_processor.options {
                options.clone()
            } else {
                MergeOption::new()
            };

            if options.remove_after_merged.unwrap_or(false) {
                match merge_processor.direction {
                    MergeDirection::Row => {
                        if let Some(target_table_names) = &merge_processor.target_table_names {
                            for target_table_name in target_table_names {
                                self.remove_data_source(session_id, target_table_name)
                                    .await?;
                            }
                        }
                    }
                    MergeDirection::Column => {
                        if let Some(targets) = &merge_processor.targets {
                            for target in targets {
                                self.remove_data_source(session_id, &target.table_name)
                                    .await?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn execute_sql(
        &self,
        session_id: &str,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, ResponseError> {
        Ok(context!(self, session_id)?.execute_sql(sql).await?)
    }
}
