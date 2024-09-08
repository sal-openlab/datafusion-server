// database/column_binder.rs: arrow data type to SQLx query bindings.
// Sasaki, Naoki <nsasaki@sal.co.jp> September 1, 2024
//

use std::sync::Arc;

use crate::data_source::database::any_pool::AnyDatabasePool;
use crate::data_source::database::engine_type::DatabaseEngineType;
use datafusion::arrow::{
    array::{
        Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        StringArray,
    },
    datatypes::DataType,
};
#[cfg(feature = "mysql")]
use sqlx::{mysql::MySqlArguments, MySql};
#[cfg(feature = "postgres")]
use sqlx::{postgres::PgArguments, Postgres};
use sqlx::{query::Query, Database, Encode, Type};

pub enum DatabaseQuery<'q> {
    #[cfg(feature = "postgres")]
    Postgres(Query<'q, Postgres, PgArguments>),
    #[cfg(feature = "mysql")]
    MySql(Query<'q, MySql, MySqlArguments>),
}

pub fn create_query<'q>(engine_type: &DatabaseEngineType, sql: &'q str) -> DatabaseQuery<'q> {
    match engine_type {
        #[cfg(feature = "postgres")]
        DatabaseEngineType::Postgres => DatabaseQuery::Postgres(sqlx::query::<Postgres>(sql)),
        #[cfg(feature = "mysql")]
        DatabaseEngineType::MySQL => DatabaseQuery::MySql(sqlx::query::<MySql>(sql)),
    }
}

pub async fn execute_query<'q>(
    query: DatabaseQuery<'q>,
    pool: &AnyDatabasePool,
) -> Result<(u64, Option<u64>), sqlx::Error> {
    match query {
        #[cfg(feature = "postgres")]
        DatabaseQuery::Postgres(q) => {
            if let AnyDatabasePool::Postgres(p) = pool {
                let result = q.execute(p.as_ref()).await?;
                Ok((result.rows_affected(), None))
            } else {
                unreachable!("Must not be reach here!")
            }
        }
        #[cfg(feature = "mysql")]
        DatabaseQuery::MySql(q) => {
            if let AnyDatabasePool::MySql(p) = pool {
                let result = q.execute(p.as_ref()).await?;
                Ok((result.rows_affected(), Some(result.last_insert_id())))
            } else {
                unreachable!("Must not be reach here!")
            }
        }
    }
}

pub fn bind_query<'q>(
    query: DatabaseQuery<'q>,
    column: &Arc<dyn Array>,
    row: usize,
) -> DatabaseQuery<'q> {
    match query {
        #[cfg(feature = "postgres")]
        DatabaseQuery::Postgres(q) => {
            let new_query = bind_from_arrow(q, column, row);
            DatabaseQuery::Postgres(new_query)
        }
        #[cfg(feature = "mysql")]
        DatabaseQuery::MySql(q) => {
            let new_query = bind_from_arrow(q, column, row);
            DatabaseQuery::MySql(new_query)
        }
    }
}

macro_rules! bind_primitive_value {
    ($query:expr, $column:expr, $row:expr, $array_type:ty, $native_type:ty) => {{
        let array = $column.as_any().downcast_ref::<$array_type>();
        if let Some(array) = array {
            let value = array.value($row);
            $query.bind(value)
        } else {
            $query.bind(None::<$native_type>)
        }
    }};
}

macro_rules! bind_string_value {
    ($query:expr, $column:expr, $row:expr) => {{
        let array = $column.as_any().downcast_ref::<StringArray>();
        if let Some(array) = array {
            let value = array.value($row).to_string();
            $query.bind(value)
        } else {
            $query.bind(None::<&str>)
        }
    }};
}

#[allow(clippy::match_same_arms)]
fn bind_from_arrow<'q, DB>(
    mut query: Query<'q, DB, <DB as Database>::Arguments<'q>>,
    column: &Arc<dyn Array>,
    row: usize,
) -> Query<'q, DB, <DB as Database>::Arguments<'q>>
where
    DB: Database,
    i8: Encode<'q, DB> + Type<DB>,
    Option<i8>: Encode<'q, DB> + Type<DB>,
    i16: Encode<'q, DB> + Type<DB>,
    Option<i16>: Encode<'q, DB> + Type<DB>,
    i32: Encode<'q, DB> + Type<DB>,
    Option<i32>: Encode<'q, DB> + Type<DB>,
    i64: Encode<'q, DB> + Type<DB>,
    Option<i64>: Encode<'q, DB> + Type<DB>,
    f32: Encode<'q, DB> + Type<DB>,
    Option<f32>: Encode<'q, DB> + Type<DB>,
    f64: Encode<'q, DB> + Type<DB>,
    Option<f64>: Encode<'q, DB> + Type<DB>,
    &'q str: Encode<'q, DB> + Type<DB>,
    Option<&'q str>: Encode<'q, DB> + Type<DB>,
    String: Encode<'q, DB> + Type<DB>,
    Option<String>: Encode<'q, DB> + Type<DB>,
{
    query = match column.data_type() {
        DataType::Int8 => bind_primitive_value!(query, column, row, Int8Array, i8),
        DataType::Int16 => bind_primitive_value!(query, column, row, Int16Array, i16),
        DataType::Int32 => bind_primitive_value!(query, column, row, Int32Array, i32),
        DataType::Int64 => bind_primitive_value!(query, column, row, Int64Array, i64),
        DataType::UInt8 => bind_primitive_value!(query, column, row, Int8Array, i8),
        DataType::UInt16 => bind_primitive_value!(query, column, row, Int16Array, i16),
        DataType::UInt32 => bind_primitive_value!(query, column, row, Int32Array, i32),
        DataType::UInt64 => bind_primitive_value!(query, column, row, Int64Array, i64),
        DataType::Float16 => bind_primitive_value!(query, column, row, Float32Array, f32),
        DataType::Float32 => bind_primitive_value!(query, column, row, Float32Array, f32),
        DataType::Float64 => bind_primitive_value!(query, column, row, Float64Array, f64),
        DataType::Utf8 => bind_string_value!(query, column, row),
        _ => unimplemented!("Unsupported data type: {}", column.data_type()),
    };

    query
}
