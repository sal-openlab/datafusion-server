// database/table_provider.ra: Table provider for external databases
// Sasaki, Naoki <nsasaki@sal.co.jp> July 27, 2024
//

use std::collections::HashMap;
use std::fmt::{Debug, Write};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::data_source::database::any_pool::DatabaseOperator;
#[cfg(feature = "mysql")]
use crate::data_source::database::dtype_mysql;
#[cfg(feature = "postgres")]
use crate::data_source::database::dtype_postgres;
use crate::data_source::database::{
    any_pool::{AnyDatabasePool, AnyDatabaseRow},
    engine_type::DatabaseEngineType,
};
use async_trait::async_trait; // TODO: Replace in the future when the Rust compiler's async trait supports object safety.
use chrono::{Datelike, Timelike};
#[cfg(feature = "mysql")]
use datafusion::arrow::array::{UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder};
use datafusion::{
    arrow::{
        array::{
            ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder,
            Decimal128Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
            Int64Builder, Int8Builder, StringBuilder, Time64MicrosecondBuilder,
            TimestampMicrosecondBuilder,
        },
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    catalog::Session,
    datasource::{memory::MemTable, TableProvider, TableType},
    error::DataFusionError,
    execution::context::SessionContext,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use futures::StreamExt;
use num_traits::ToPrimitive;

const BATCH_SIZE: usize = 1000;

#[derive(Debug)]
pub struct DatabaseTable {
    pool: AnyDatabasePool,
    schema: SchemaRef,
    table_name: String,
}

#[async_trait]
impl TableProvider for DatabaseTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        _state: &'life1 (dyn Session + 'life1),
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Arc<dyn ExecutionPlan>, DataFusionError>>
                + Send
                + 'async_trait,
        >,
    >
    where
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
    {
        let table_name = self.table_name.clone();
        let pool = self.pool.clone();
        let schema = self.schema.clone();
        let projection = projection.cloned();
        let filters = filters.to_vec();

        Box::pin(async move {
            let mut sql = format!("SELECT * FROM {table_name}");

            if !filters.is_empty() {
                let filter_clauses: Vec<String> = filters.iter().map(Expr::to_string).collect();
                if !filter_clauses.is_empty() {
                    write!(&mut sql, " WHERE {}", filter_clauses.join(" AND "))?;
                }
            }

            let projected_fields = if let Some(projection) = projection {
                let columns: Vec<String> = projection
                    .iter()
                    .map(|index| schema.field(*index).name().clone())
                    .collect();
                sql = sql.replace('*', &columns.join(", "));
                columns
            } else {
                schema.fields().iter().map(|f| f.name().clone()).collect()
            };

            let projected_schema = Arc::new(Schema::new(
                projected_fields
                    .iter()
                    .map(|name| schema.field_with_name(name).unwrap().clone())
                    .collect::<Vec<_>>(),
            ));

            if let Some(limit) = limit {
                write!(&mut sql, " LIMIT {limit}")?;
            }

            // retrieve from external database system
            let mut stream = pool.fetch(&sql);
            let mut builders = Self::create_column_builders(&projected_schema)?;
            let mut row_count = 0;
            let mut record_batches = vec![];

            while let Some(row) = stream.next().await {
                let row = row.map_err(|e| DataFusionError::Execution(e.to_string()))?;
                row_count += 1;

                for (index, name) in projected_fields.iter().enumerate() {
                    Self::append_value_to_builder(
                        &mut builders[index],
                        schema.field_with_name(name)?,
                        schema.metadata.get(name).unwrap_or(&String::new()),
                        &row,
                    )?;
                }

                if row_count == BATCH_SIZE {
                    let arrays: Vec<ArrayRef> = builders
                        .into_iter()
                        .map(|mut builder| builder.finish())
                        .collect();

                    record_batches.push(RecordBatch::try_new(projected_schema.clone(), arrays)?);

                    builders = Self::create_column_builders(&projected_schema)?;
                    row_count = 0;
                }
            }

            if row_count > 0 {
                let arrays: Vec<ArrayRef> = builders
                    .into_iter()
                    .map(|mut builder| builder.finish())
                    .collect();

                record_batches.push(RecordBatch::try_new(projected_schema.clone(), arrays)?);
            }

            let memory_table = Arc::new(MemTable::try_new(
                projected_schema.clone(),
                vec![record_batches.clone()],
            )?);

            let ctx = SessionContext::new();
            ctx.register_table("table", memory_table)?;
            let dataframe = ctx.table("table").await?;

            dataframe.create_physical_plan().await
        })
    }
}

macro_rules! append_value {
    ($builder:expr, $field:expr, $row:expr, $type:ty, $builder_type:ty) => {{
        let builder = $builder.as_any_mut().downcast_mut::<$builder_type>();
        match builder {
            Some(builder) => {
                if let Some(value) = $row.get::<$type>($field.name()) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
                Ok(())
            }
            None => Err(DataFusionError::Internal(format!(
                "Failed to downcast builder for field '{}'",
                $field.name()
            ))),
        }
    }};
}

#[cfg(feature = "mysql")]
macro_rules! append_mysql_specific_value {
    ($builder:expr, $field:expr, $row:expr, $type:ty, $builder_type:ty) => {{
        let builder = $builder.as_any_mut().downcast_mut::<$builder_type>();
        match builder {
            Some(builder) => {
                if let Some(value) = $row.get_mysql::<$type>($field.name()) {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
                Ok(())
            }
            None => Err(DataFusionError::Internal(format!(
                "Failed to downcast builder for field '{}'",
                $field.name()
            ))),
        }
    }};
}

impl DatabaseTable {
    pub async fn new(
        engine_type: &DatabaseEngineType,
        pool: AnyDatabasePool,
        database: &str,
        table_name: &str,
    ) -> Result<Self, DataFusionError> {
        log::debug!("Inspecting external database schema: database={database}, table={table_name}");

        let sql = match engine_type {
            #[cfg(feature = "postgres")]
            DatabaseEngineType::Postgres => format!(
                "SELECT column_name, data_type, numeric_precision, numeric_scale \
                FROM information_schema.columns \
                WHERE table_name='{table_name}'",
            ),
            #[cfg(feature = "mysql")]
            DatabaseEngineType::MySQL => format!(
                "SELECT column_name, data_type, column_type, numeric_precision, numeric_scale \
                FROM information_schema.columns \
                WHERE table_schema='{database}' AND table_name='{table_name}'",
            ),
        };

        log::debug!("Retrieving schema: {sql}");

        let rows = pool
            .fetch_all(&sql)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        log::debug!("Result schema information records: {}", rows.len());

        #[allow(clippy::type_complexity)]
        let columns: Vec<(String, String, Option<i16>, Option<i8>, bool)> = rows
            .iter()
            .map(|row| {
                (
                    row.get::<String>("column_name").unwrap_or_default(),
                    row.get::<String>("data_type").unwrap_or_default(),
                    row.get::<i16>("numeric_precision"),
                    row.get::<i8>("numeric_scale"),
                    match engine_type {
                        #[cfg(feature = "postgres")]
                        DatabaseEngineType::Postgres => true,
                        #[cfg(feature = "mysql")]
                        DatabaseEngineType::MySQL => !row
                            .get::<String>("column_type")
                            .unwrap_or_default()
                            .ends_with("unsigned"),
                    },
                )
            })
            .collect();

        // stores original dtype name
        let meta_info: HashMap<String, String> = columns
            .clone()
            .into_iter()
            .map(|(column_name, dtype, ..)| (column_name, dtype))
            .collect();

        let fields: Vec<Field> = columns
            .into_iter()
            .map(|(column_name, dtype, precision, scale, signed)| {
                let arrow_dtype = match engine_type {
                    #[cfg(feature = "postgres")]
                    DatabaseEngineType::Postgres => {
                        dtype_postgres::to_arrow_dtype(&dtype, precision, scale, signed)
                    }
                    #[cfg(feature = "mysql")]
                    DatabaseEngineType::MySQL => {
                        dtype_mysql::to_arrow_dtype(&dtype, precision, scale, signed)
                    }
                };
                Field::new(&column_name, arrow_dtype, true)
            })
            .collect();

        let schema = Arc::new(Schema::new_with_metadata(fields, meta_info));

        log::debug!("Established schema: {schema:?}");

        Ok(DatabaseTable {
            pool,
            schema,
            table_name: table_name.to_string(),
        })
    }

    fn create_column_builders(
        projected_schema: &Schema,
    ) -> Result<Vec<Box<dyn ArrayBuilder>>, DataFusionError> {
        let mut builders = vec![];

        for field in projected_schema.fields() {
            builders.push(match field.data_type() {
                DataType::Boolean => {
                    Box::new(BooleanBuilder::with_capacity(BATCH_SIZE)) as Box<dyn ArrayBuilder>
                }
                DataType::Int8 => {
                    Box::new(Int8Builder::with_capacity(BATCH_SIZE)) as Box<dyn ArrayBuilder>
                }
                DataType::Int16 => {
                    Box::new(Int16Builder::with_capacity(BATCH_SIZE)) as Box<dyn ArrayBuilder>
                }
                DataType::Int32 => {
                    Box::new(Int32Builder::with_capacity(BATCH_SIZE)) as Box<dyn ArrayBuilder>
                }
                DataType::Int64 => {
                    Box::new(Int64Builder::with_capacity(BATCH_SIZE)) as Box<dyn ArrayBuilder>
                }
                #[cfg(feature = "mysql")]
                DataType::UInt8 => {
                    Box::new(UInt8Builder::with_capacity(BATCH_SIZE)) as Box<dyn ArrayBuilder>
                }
                #[cfg(feature = "mysql")]
                DataType::UInt16 => {
                    Box::new(UInt16Builder::with_capacity(BATCH_SIZE)) as Box<dyn ArrayBuilder>
                }
                #[cfg(feature = "mysql")]
                DataType::UInt32 => {
                    Box::new(UInt32Builder::with_capacity(BATCH_SIZE)) as Box<dyn ArrayBuilder>
                }
                #[cfg(feature = "mysql")]
                DataType::UInt64 => {
                    Box::new(UInt64Builder::with_capacity(BATCH_SIZE)) as Box<dyn ArrayBuilder>
                }
                DataType::Float32 => {
                    Box::new(Float32Builder::with_capacity(BATCH_SIZE)) as Box<dyn ArrayBuilder>
                }
                DataType::Float64 => {
                    Box::new(Float64Builder::with_capacity(BATCH_SIZE)) as Box<dyn ArrayBuilder>
                }
                DataType::Decimal128(precision, scale) => Box::new(
                    Decimal128Builder::with_capacity(BATCH_SIZE)
                        .with_precision_and_scale(*precision, *scale)?,
                )
                    as Box<dyn ArrayBuilder>,
                DataType::Utf8 => {
                    Box::new(StringBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 50))
                        as Box<dyn ArrayBuilder>
                }
                DataType::Binary => {
                    Box::new(BinaryBuilder::with_capacity(BATCH_SIZE, BATCH_SIZE * 256))
                        as Box<dyn ArrayBuilder>
                }
                DataType::Timestamp(TimeUnit::Microsecond, timezone) => Box::new(
                    TimestampMicrosecondBuilder::with_capacity(BATCH_SIZE)
                        .with_timezone_opt(timezone.clone()),
                )
                    as Box<dyn ArrayBuilder>,
                DataType::Date32 => {
                    Box::new(Date32Builder::with_capacity(BATCH_SIZE)) as Box<dyn ArrayBuilder>
                }
                DataType::Time64(TimeUnit::Microsecond) => {
                    Box::new(Time64MicrosecondBuilder::with_capacity(BATCH_SIZE))
                        as Box<dyn ArrayBuilder>
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported data type: {:?}",
                        field.data_type()
                    )))
                }
            });
        }

        Ok(builders)
    }

    fn append_value_to_builder(
        builder: &mut Box<dyn ArrayBuilder>,
        field: &Field,
        original_dtype: &str,
        row: &AnyDatabaseRow,
    ) -> Result<(), DataFusionError> {
        match field.data_type() {
            DataType::Boolean => append_value!(builder, field, row, bool, BooleanBuilder)?,
            DataType::Int8 => append_value!(builder, field, row, i8, Int8Builder)?,
            DataType::Int16 => append_value!(builder, field, row, i16, Int16Builder)?,
            DataType::Int32 => append_value!(builder, field, row, i32, Int32Builder)?,
            DataType::Int64 => append_value!(builder, field, row, i64, Int64Builder)?,
            #[cfg(feature = "mysql")]
            DataType::UInt8 => {
                append_mysql_specific_value!(builder, field, row, u8, UInt8Builder)?;
            }
            #[cfg(feature = "mysql")]
            DataType::UInt16 => {
                append_mysql_specific_value!(builder, field, row, u16, UInt16Builder)?;
            }
            #[cfg(feature = "mysql")]
            DataType::UInt32 => {
                append_mysql_specific_value!(builder, field, row, u32, UInt32Builder)?;
            }
            #[cfg(feature = "mysql")]
            DataType::UInt64 => {
                append_mysql_specific_value!(builder, field, row, u64, UInt64Builder)?;
            }
            DataType::Float32 => append_value!(builder, field, row, f32, Float32Builder)?,
            DataType::Float64 => append_value!(builder, field, row, f64, Float64Builder)?,
            DataType::Decimal128(_precision, scale) => {
                if let Some(builder) = builder.as_any_mut().downcast_mut::<Decimal128Builder>() {
                    if let Some(value) = row.get::<sqlx::types::Decimal>(field.name()) {
                        #[allow(clippy::cast_sign_loss)]
                        let scale_factor = sqlx::types::Decimal::new(1, *scale as u32);
                        let scaled_value = (value / scale_factor).to_i128().unwrap();
                        builder.append_value(scaled_value);
                    } else {
                        builder.append_null();
                    }
                }
            }
            DataType::Utf8 => match original_dtype {
                #[cfg(feature = "postgres")]
                "uuid" => {
                    if let Some(builder) = builder.as_any_mut().downcast_mut::<StringBuilder>() {
                        if let Some(uuid) = row.get::<sqlx::types::Uuid>(field.name()) {
                            builder.append_value(uuid.to_string());
                        } else {
                            builder.append_null();
                        }
                    }
                }
                _ => append_value!(builder, field, row, String, StringBuilder)?,
            },
            DataType::Binary => append_value!(builder, field, row, Vec<u8>, BinaryBuilder)?,
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                if let Some(builder) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                {
                    if let Some(ts) = row.get::<chrono::DateTime<chrono::Utc>>(field.name()) {
                        builder.append_value(ts.timestamp_micros());
                    } else {
                        builder.append_null();
                    }
                }
            }
            DataType::Date32 => {
                if let Some(builder) = builder.as_any_mut().downcast_mut::<Date32Builder>() {
                    if let Some(nd) = row.get::<chrono::NaiveDate>(field.name()) {
                        builder.append_value(nd.num_days_from_ce() - 719_163 /* 1970-01-01 */);
                    } else {
                        builder.append_null();
                    }
                }
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                if let Some(builder) = builder
                    .as_any_mut()
                    .downcast_mut::<Time64MicrosecondBuilder>()
                {
                    if let Some(nt) = row.get::<chrono::NaiveTime>(field.name()) {
                        builder.append_value(
                            i64::from(nt.num_seconds_from_midnight()) * 1_000_000
                                + i64::from(nt.nanosecond()) / 1_000,
                        );
                    } else {
                        builder.append_null();
                    }
                }
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported data type for field '{}'",
                    field.name()
                )))
            }
        }

        Ok(())
    }
}
