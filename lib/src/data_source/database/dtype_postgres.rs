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
        "double precision" => DataType::Float64,
        "character varying" | "character" | "bpchar" | "text" => DataType::Utf8,
        "bit varying" | "bit" => DataType::Utf8,
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
        // TODO: to be resolved year-month problem in interval
        // * Internally in Postgres, they are managed in `months`, `days`, and `microseconds`.
        //   This can be accessed with the PgInterval type of SQLx.
        // * Postgres SQL interface has fields `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `SECOND`.
        //   For `SECOND`, it can specify a precision from 0 to 6 (seconds to microseconds),
        //   expressed as a fraction of a second.
        //   The data type and precision can be defined from the `interval_type` and `interval_precision`
        //   fields in `information_schema.columns`.
        // * For `INTERVAL`, the field specification must be in continuous time units,
        //   such as `INTERVAL YEAR`, `INTERVAL YEAR TO MONTH` or `INTERVAL MONTH TO DAY`.
        //   By default (`INTERVAL`), all fields are valid.
        // * If `INTERVAL DAY TO HOUR` is defined as the upper limit,
        //   or if more than `MONTH` fields are used in the default (all fields),
        //   they cannot simply be expressed by Arrow's `Duration(TimeUnit=Microseconds)`.
        //   Besides, if it is expressed by `Interval`, it requires two columns with `YearMonth` and
        //   `DayTime` specified in `IntervalUnit` respectively.
        "interval" => DataType::Utf8,
        "boolean" => DataType::Boolean,
        "money" => DataType::Utf8,
        "uuid" => DataType::Utf8,
        "json" | "jsonb" => DataType::Utf8,
        "xml" => DataType::Utf8,
        "inet" | "cidr" | "macaddr" | "macaddr8" => DataType::Utf8,
        "point" | "line" | "lseg" | "box" | "circle" | "polygon" | "path" => DataType::Utf8, // TODO: to be converted `List` or `Struct`
        _ => unimplemented!("Unsupported data type: {}", postgres_dtype),
    }
}
