// database/dtype_postgres.rs: Database connection pool manager
// Sasaki, Naoki <nsasaki@sal.co.jp> July 28, 2024
//

#[cfg(feature = "postgres")]
use datafusion::arrow::datatypes::{DataType, TimeUnit};

#[cfg(feature = "postgres")]
pub fn to_arrow_dtype(
    postgres_dtype: &str,
    precision: Option<i16>,
    scale: Option<i8>,
    _signed: bool,
) -> DataType {
    #[allow(clippy::match_same_arms)]
    match postgres_dtype {
        "smallint" | "smallserial" => DataType::Int16,
        "integer" | "serial" => DataType::Int32,
        "bigint" | "bigserial" => DataType::Int64,
        "numeric" => DataType::Decimal128(
            u8::try_from(precision.unwrap_or(38)).unwrap_or(255),
            scale.unwrap_or(10),
        ),
        "real" => DataType::Float32,
        "double precision" | "money" => DataType::Float64,
        "character varying" | "character" | "bpchar" | "text" => DataType::Utf8,
        "bytea" => DataType::Binary,
        "timestamp with time zone" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        "timestamp" | "timestamp without time zone" => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        "date" => DataType::Date32,
        "time with time zone" => DataType::Time64(TimeUnit::Microsecond),
        "time" | "time without time zone" => DataType::Time64(TimeUnit::Microsecond),
        // "interval" => DataType::Utf8,
        "boolean" => DataType::Boolean,
        "uuid" => DataType::Utf8,
        _ => unimplemented!("Unsupported data type: {}", postgres_dtype),
    }
}
