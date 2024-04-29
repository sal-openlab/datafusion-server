// session.rs - Session context for DataFusion's execution runtime environment.
// Sasaki, Naoki <nsasaki@sal.co.jp> January 14, 2023
//

use std::collections::HashMap;

use axum::async_trait;
use chrono::{DateTime, Utc};
#[cfg(feature = "avro")]
use datafusion::datasource::file_format::options::AvroReadOptions;
use datafusion::{
    arrow::{compute, datatypes::SchemaRef, record_batch::RecordBatch},
    dataframe::DataFrame,
    execution::context,
    logical_expr::{col, JoinType},
};
use tokio::sync::RwLock;

#[cfg(feature = "plugin")]
use crate::data_source::connector_plugin;
#[cfg(feature = "flight")]
use crate::data_source::flight_stream;
use crate::data_source::{csv, json, location_uri, nd_json_file, nd_json_rest, parquet, writer};
#[cfg(feature = "plugin")]
use crate::request::body::PluginOption;
use crate::request::body::{
    DataSource, DataSourceFormat, DataSourceOption, MergeDirection, MergeOption, MergeProcessor,
};
use crate::response::http_error::ResponseError;
use crate::settings::Settings;

#[allow(clippy::module_name_repetitions)]
pub struct SessionContext {
    df_ctx: context::SessionContext,
    last_accessed_at: DateTime<Utc>,
    keep_alive: i64,
    data_source_map: HashMap<String, DataSource>,
}

impl SessionContext {
    pub fn new(keep_alive: Option<i64>) -> Self {
        Self::new_with_config(context::SessionConfig::default(), keep_alive)
    }

    pub fn new_with_config(config: context::SessionConfig, keep_alive: Option<i64>) -> Self {
        let df_ctx = context::SessionContext::new_with_config(config);
        let last_accessed_at = Utc::now();
        let data_source_map = HashMap::<String, DataSource>::new();

        let keep_alive = if let Some(keep_alive) = keep_alive {
            keep_alive
        } else {
            Settings::global().session.default_keep_alive
        } * 1000;

        Self {
            df_ctx,
            last_accessed_at,
            keep_alive,
            data_source_map,
        }
    }
}

// TODO: to be used non concurrent version of `SessionContext` when not sharable context with sessions
pub type ConcurrentSessionContext = RwLock<SessionContext>;

#[async_trait]
pub trait Session: Send + Sync + 'static {
    async fn id(&self) -> String;
    async fn session_start_time(&self) -> DateTime<Utc>;
    async fn ttl(&self) -> i64;
    async fn touch(&self);
    async fn expired(&self) -> bool;
    async fn schema_ref(&self, name: &str) -> Option<SchemaRef>;
    async fn data_source_names(&self) -> Vec<String>;
    async fn data_source(
        &self,
        name: &str,
    ) -> Result<(Option<DataSource>, SchemaRef), ResponseError>;
    async fn exists_data_source(&self, data_source: &DataSource) -> Result<(), ResponseError>;
    async fn register_record_batch(
        &self,
        data_source: &DataSource,
        record_batches: &[RecordBatch],
    ) -> Result<(), ResponseError>;
    async fn append_from_csv_file(&self, data_source: &DataSource) -> Result<(), ResponseError>;
    async fn append_from_csv_bytes(
        &self,
        name: &str,
        data: bytes::Bytes,
    ) -> Result<(), ResponseError>;
    async fn append_from_json_file(&self, data_source: &DataSource) -> Result<(), ResponseError>;
    async fn append_from_json_rest(&self, data_source: &DataSource) -> Result<(), ResponseError>;
    async fn append_from_json_bytes(
        &self,
        name: &str,
        data: bytes::Bytes,
    ) -> Result<(), ResponseError>;
    #[cfg(feature = "avro")]
    async fn append_from_avro_file(&self, data_source: &DataSource) -> Result<(), ResponseError>;
    #[cfg(feature = "flight")]
    async fn append_from_flight_stream(
        &self,
        data_source: &DataSource,
    ) -> Result<(), ResponseError>;
    #[cfg(feature = "plugin")]
    async fn append_from_connector_plugin(
        &self,
        data_source: &DataSource,
    ) -> Result<(), ResponseError>;
    async fn append_from_parquet_file(&self, data_source: &DataSource)
        -> Result<(), ResponseError>;
    async fn append_from_parquet_bytes(
        &self,
        name: &str,
        data: bytes::Bytes,
    ) -> Result<(), ResponseError>;
    async fn save_to_file(&self, data_source: &DataSource) -> Result<(), ResponseError>;
    async fn remove_data_source(&self, name: &str) -> Result<(), ResponseError>;
    async fn execute_merge_processor(
        &self,
        merge_processor: &MergeProcessor,
    ) -> Result<(), ResponseError>;
    async fn execute_logical_plan(&self, sql: &str) -> Result<DataFrame, ResponseError>;
}

#[async_trait]
impl Session for ConcurrentSessionContext {
    async fn id(&self) -> String {
        self.read().await.df_ctx.session_id()
    }

    async fn session_start_time(&self) -> DateTime<Utc> {
        self.read().await.df_ctx.session_start_time()
    }

    async fn ttl(&self) -> i64 {
        let session = &mut self.read().await;
        let current_timestamp = Utc::now().timestamp_millis();
        session.last_accessed_at.timestamp_millis() - current_timestamp + session.keep_alive
    }

    /// extends session TTL
    async fn touch(&self) {
        let session = &mut self.write().await;
        session.last_accessed_at = Utc::now();
    }

    async fn expired(&self) -> bool {
        self.ttl().await <= 0
    }

    async fn schema_ref(&self, name: &str) -> Option<SchemaRef> {
        let session = &mut self.read().await;
        if let Ok(provider) = session.df_ctx.table_provider(name).await {
            Some(provider.schema())
        } else {
            None
        }
    }

    async fn data_source_names(&self) -> Vec<String> {
        let session = &mut self.read().await;
        let catalog = session
            .df_ctx
            .catalog(&session.df_ctx.catalog_names()[0]) // just exists `datafusion`
            .unwrap();
        catalog
            .schema(&catalog.schema_names()[0]) // just exists `public`
            .unwrap()
            .table_names()
    }

    async fn data_source(
        &self,
        name: &str,
    ) -> Result<(Option<DataSource>, SchemaRef), ResponseError> {
        let session = &mut self.read().await;
        let schema = session
            .df_ctx
            .table_provider(name)
            .await
            .map_err(|e| {
                ResponseError::request_validation(format!(
                    "Not found data source name '{name}': {e}"
                ))
            })?
            .schema();

        Ok((session.data_source_map.get(name).cloned(), schema))
    }

    async fn exists_data_source(&self, data_source: &DataSource) -> Result<(), ResponseError> {
        let session = &mut self.write().await;

        if session
            .df_ctx
            .table_provider(&data_source.name)
            .await
            .is_ok()
        {
            log::debug!("Duplicated data source '{}' in context", data_source.name);

            let options = match &data_source.options {
                Some(options) => options.clone(),
                None => DataSourceOption::new_with_default(),
            };

            if !options.overwrite.unwrap_or(false) {
                return Err(ResponseError::request_validation(format!(
                    "Duplicated data source '{}'",
                    data_source.name
                )));
            }

            log::debug!("Removing data source '{}' from context", data_source.name);

            session.df_ctx.deregister_table(&data_source.name)?;
            session.data_source_map.remove(&data_source.name);
        }

        Ok(())
    }

    async fn register_record_batch(
        &self,
        data_source: &DataSource,
        record_batches: &[RecordBatch],
    ) -> Result<(), ResponseError> {
        if record_batches.is_empty() {
            log::debug!("Can not register empty record batch, require schema information");
            return Err(ResponseError::request_validation("empty record batch"));
        }

        log::debug!(
            "Register record batch to session context: number of record batches {}",
            record_batches.len()
        );

        self.exists_data_source(data_source).await?;

        self.touch().await; // Important that extends the expiry of session TTL here.
        {
            let session = &mut self.write().await;

            let record_batch =
                compute::concat_batches(&record_batches[0].schema(), record_batches)?;

            session
                .df_ctx
                .register_batch(&data_source.name, record_batch)?;

            session
                .data_source_map
                .insert(data_source.name.clone(), data_source.clone());
        }

        log::debug!("Registered data source '{}' to context", data_source.name);

        Ok(())
    }

    async fn append_from_csv_file(&self, data_source: &DataSource) -> Result<(), ResponseError> {
        let file_path = create_data_file_path(&data_source.location)?;
        log::debug!("Reading CSV file {file_path:?}");

        let options = match &data_source.options {
            Some(options) => options.clone(),
            None => DataSourceOption::new_with_default(),
        };

        let record_batches =
            csv::from_file_to_record_batch(&file_path, &data_source.schema, &options)?;

        Self::register_record_batch(self, data_source, &record_batches).await?;

        Ok(())
    }

    async fn append_from_csv_bytes(
        &self,
        name: &str,
        data: bytes::Bytes,
    ) -> Result<(), ResponseError> {
        let data_source = DataSource::new(DataSourceFormat::Csv, name, None);
        let options = DataSourceOption::new_with_default().with_infer_schema_rows(1000);

        Self::register_record_batch(
            self,
            &data_source,
            &csv::from_bytes_to_record_batch(data, &options)?,
        )
        .await?;

        Ok(())
    }

    async fn append_from_json_file(&self, data_source: &DataSource) -> Result<(), ResponseError> {
        let file_path = create_data_file_path(&data_source.location)?;
        log::debug!("Reading JSON file {file_path:?}");

        let options = match &data_source.options {
            Some(o) => o.clone(),
            None => DataSourceOption::new_with_default(),
        };

        let record_batches = match &data_source.format {
            DataSourceFormat::Json => {
                json::from_file_to_record_batch(&file_path, &data_source.schema, &options)?
            }
            DataSourceFormat::NdJson => {
                nd_json_file::to_record_batch(&file_path, &data_source.schema, &options)?
            }
            _ => {
                return Err(ResponseError::internal_server_error(
                    "Unrecognized data source format configuration",
                ));
            }
        };

        Self::register_record_batch(self, data_source, &record_batches).await?;

        Ok(())
    }

    async fn append_from_json_rest(&self, data_source: &DataSource) -> Result<(), ResponseError> {
        let options = match &data_source.options {
            Some(o) => o.clone(),
            None => DataSourceOption::new_with_default(),
        };

        let record_batches = match &data_source.format {
            DataSourceFormat::Json => {
                json::from_response_to_record_batch(
                    &data_source.location,
                    &data_source.schema,
                    &options,
                )
                .await?
            }
            DataSourceFormat::NdJson => {
                nd_json_rest::to_record_batch(&data_source.location, &data_source.schema, &options)
                    .await?
            }
            _ => {
                return Err(ResponseError::internal_server_error(
                    "Unrecognized data source format configuration",
                ));
            }
        };

        Self::register_record_batch(self, data_source, &record_batches).await?;

        Ok(())
    }

    async fn append_from_json_bytes(
        &self,
        name: &str,
        data: bytes::Bytes,
    ) -> Result<(), ResponseError> {
        let data_source = DataSource::new(DataSourceFormat::Json, name, None);
        let options = DataSourceOption::new_with_default();

        Self::register_record_batch(
            self,
            &data_source,
            &json::from_bytes_to_record_batch(data, &options)?,
        )
        .await?;

        Ok(())
    }

    #[cfg(feature = "avro")]
    async fn append_from_avro_file(&self, data_source: &DataSource) -> Result<(), ResponseError> {
        let file_path = create_data_file_path(&data_source.location)?;
        log::debug!("Reading Avro file {file_path:?}");

        self.exists_data_source(data_source).await?;

        self.touch().await;
        {
            let session = &mut self.write().await;
            session
                .df_ctx
                .register_avro(&data_source.name, &file_path, AvroReadOptions::default())
                .await?;
            session
                .data_source_map
                .insert(data_source.name.clone(), data_source.clone());
        }

        Ok(())
    }

    #[cfg(feature = "flight")]
    async fn append_from_flight_stream(
        &self,
        data_source: &DataSource,
    ) -> Result<(), ResponseError> {
        let options = match &data_source.options {
            Some(o) => o.clone(),
            None => DataSourceOption::new_with_default(),
        };

        let record_batches =
            flight_stream::to_record_batch(&data_source.location, &options).await?;

        Self::register_record_batch(self, data_source, &record_batches).await?;

        Ok(())
    }

    #[cfg(feature = "plugin")]
    async fn append_from_connector_plugin(
        &self,
        data_source: &DataSource,
    ) -> Result<(), ResponseError> {
        {
            let options = match &data_source.options {
                Some(o) => o.clone(),
                None => DataSourceOption::new_with_default(),
            };

            let plugin_options = match &data_source.plugin_options {
                Some(o) => o.clone(),
                None => PluginOption::new(),
            };

            let record_batches = connector_plugin::to_record_batch(
                &data_source.format,
                &data_source.location,
                &data_source.schema,
                &options,
                &plugin_options,
            )?;

            Self::register_record_batch(self, data_source, &record_batches).await?;
        }

        Ok(())
    }

    async fn append_from_parquet_file(
        &self,
        data_source: &DataSource,
    ) -> Result<(), ResponseError> {
        let file_path = create_data_file_path(&data_source.location)?;
        log::debug!("Reading Parquet file {file_path:?}");

        Self::register_record_batch(
            self,
            data_source,
            &parquet::from_file_to_record_batch(&file_path)?,
        )
        .await?;

        Ok(())
    }

    async fn append_from_parquet_bytes(
        &self,
        name: &str,
        data: bytes::Bytes,
    ) -> Result<(), ResponseError> {
        let data_source = DataSource::new(DataSourceFormat::Parquet, name, None);

        Self::register_record_batch(
            self,
            &data_source,
            &parquet::from_bytes_to_record_batch(data)?,
        )
        .await?;

        Ok(())
    }

    async fn save_to_file(&self, data_source: &DataSource) -> Result<(), ResponseError> {
        let mut file_path = std::path::PathBuf::from(&Settings::global().server.data_dir);
        file_path.push(location_uri::to_file_path_and_name(&data_source.location)?);

        let options = match &data_source.options {
            Some(options) => options.clone(),
            None => DataSourceOption::new_with_default(),
        };

        if !options.overwrite.unwrap_or(false) && file_path.exists() {
            return Err(ResponseError::already_existing(format!(
                "Already exists '{}' in local file system",
                &data_source.location
            )));
        }

        if let Some(path) = file_path.as_path().parent() {
            writer::fs::mkdir_if_not_exists(path, true)?;
        }

        log::debug!("save record batches to {:?}", file_path.to_str().unwrap());

        self.touch().await;
        {
            let session = &mut self.read().await;
            let data_frame = session.df_ctx.table(&data_source.name).await?;

            match data_source.format {
                DataSourceFormat::Parquet => {
                    writer::parquet_file::write(
                        &data_frame.collect().await?,
                        file_path.to_str().unwrap(),
                    )?;
                }
                DataSourceFormat::Csv => {
                    writer::csv_file::write(
                        &data_frame.collect().await?,
                        file_path.to_str().unwrap(),
                        &options,
                    )?;
                }
                DataSourceFormat::NdJson => {
                    writer::raw_json_file::write(
                        &data_frame.collect().await?,
                        file_path.to_str().unwrap(),
                    )?;
                }
                DataSourceFormat::Json => {
                    writer::json_file::write(
                        &data_frame.collect().await?,
                        file_path.to_str().unwrap(),
                    )?;
                }
                #[cfg(feature = "avro")]
                DataSourceFormat::Avro => {
                    return Err(ResponseError::unsupported_type(
                        "Save feature currently not supported 'Avro'",
                    ));
                }
                #[cfg(feature = "flight")]
                DataSourceFormat::Flight => {
                    return Err(ResponseError::unsupported_type(
                        "Not supported format 'flight' to save local file system",
                    ));
                }
                DataSourceFormat::Arrow => {
                    return Err(ResponseError::unsupported_type(
                        "Not supported format 'arrow' to save local file system",
                    ));
                }
            }
        }

        Ok(())
    }

    async fn remove_data_source(&self, name: &str) -> Result<(), ResponseError> {
        self.touch().await;
        {
            let session = &mut self.write().await;

            if session.df_ctx.table_provider(name).await.is_err() {
                return Err(ResponseError::request_validation(format!(
                    "Data source '{name}' not found"
                )));
            }

            session.df_ctx.deregister_table(name)?;
            session.data_source_map.remove(name);
        }

        Ok(())
    }

    async fn execute_merge_processor(
        &self,
        merge_processor: &MergeProcessor,
    ) -> Result<(), ResponseError> {
        let options = if let Some(options) = &merge_processor.options {
            options.clone()
        } else {
            MergeOption::new()
        };

        self.touch().await;
        {
            let session = &mut self.read().await;
            let df_ctx = &session.df_ctx;
            let mut base_table = df_ctx.table(&merge_processor.base_table_name).await?;

            match merge_processor.direction {
                MergeDirection::Column => {
                    if let Some(targets) = &merge_processor.targets {
                        let base_field_names =
                            base_table.schema().clone().strip_qualifiers().field_names();

                        for target in targets {
                            let target_table = session.df_ctx.table(&target.table_name).await?;
                            let target_field_names = target_table
                                .schema()
                                .clone()
                                .strip_qualifiers()
                                .field_names();

                            let mut projection_fields = vec![];
                            for field_name in &target_field_names {
                                projection_fields.push(if base_field_names.contains(field_name) {
                                    col(field_name)
                                        .alias(&format!("{}_{}", &target.table_name, &field_name))
                                } else {
                                    col(field_name)
                                });
                            }

                            let mut base_keys: Vec<&str> = vec![];
                            for key in &target.base_keys {
                                base_keys.push(key);
                            }

                            let mut target_aliased_keys: Vec<String> = vec![];
                            for key in &target.target_keys {
                                target_aliased_keys.push(if base_field_names.contains(key) {
                                    format!("{}_{key}", &target.table_name)
                                } else {
                                    key.clone()
                                });
                            }

                            let mut target_keys: Vec<&str> = vec![];
                            for key in &target_aliased_keys {
                                target_keys.push(key);
                            }

                            base_table = base_table
                                .join(
                                    target_table.select(projection_fields)?,
                                    JoinType::Inner,
                                    base_keys.as_slice(),
                                    target_keys.as_slice(),
                                    None,
                                )?
                                .select({
                                    let mut fields = vec![];
                                    for field_name in &base_field_names {
                                        fields.push(col(field_name));
                                    }
                                    for field_name in &target_field_names {
                                        if !target.target_keys.contains(field_name) {
                                            fields.push(col(field_name));
                                        }
                                    }
                                    fields
                                })?;
                        }
                    }
                }
                MergeDirection::Row => {
                    if let Some(targets) = &merge_processor.target_table_names {
                        for target in targets {
                            let target_table = session.df_ctx.table(target).await?;
                            base_table = if options.distinct.unwrap_or(false) {
                                base_table.union_distinct(target_table)?
                            } else {
                                base_table.union(target_table)?
                            };
                        }
                    }
                }
            }

            df_ctx.deregister_table(&merge_processor.base_table_name)?;

            let record_batches = base_table.collect().await?;
            df_ctx.register_batch(
                &merge_processor.base_table_name,
                compute::concat_batches(&record_batches[0].schema(), &record_batches)?,
            )?;
        }

        Ok(())
    }

    async fn execute_logical_plan(&self, sql: &str) -> Result<DataFrame, ResponseError> {
        self.touch().await;
        let context = &self.read().await.df_ctx;
        Ok(context.sql(sql).await?)
    }
}

fn create_data_file_path(file: &str) -> Result<String, ResponseError> {
    let mut file_path = std::path::PathBuf::from(&Settings::global().server.data_dir);
    file_path.push(location_uri::to_file_path_and_name(file)?);
    match file_path.to_str() {
        Some(v) => Ok(v.to_owned()),
        None => Err(ResponseError::unsupported_format(format!(
            "Can not decode file path string {file:?}"
        ))),
    }
}
