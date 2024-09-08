// database/dtype_mysql.rs: MySQL data type mapping
// Sasaki, Naoki <nsasaki@sal.co.jp> July 28, 2024
//

#[cfg(feature = "mysql")]
use datafusion::arrow::datatypes::{DataType, TimeUnit};

#[cfg(feature = "mysql")]
pub fn to_arrow_dtype(
    mysql_dtype: &str,
    precision: Option<i16>,
    scale: Option<i8>,
    signed: bool,
) -> DataType {
    #[allow(clippy::match_same_arms)]
    match mysql_dtype {
        "tinyint" => {
            if signed {
                DataType::Int8
            } else {
                DataType::UInt8
            }
        }
        "smallint" => {
            if signed {
                DataType::Int16
            } else {
                DataType::UInt16
            }
        }
        "int" | "mediumint" => {
            if signed {
                DataType::Int32
            } else {
                DataType::UInt32
            }
        }
        "bigint" => {
            if signed {
                DataType::Int64
            } else {
                DataType::UInt64
            }
        }
        #[allow(clippy::cast_sign_loss)]
        "decimal" => DataType::Decimal128(
            u8::try_from(precision.unwrap_or(38)).unwrap_or(255),
            scale.unwrap_or(10),
        ),
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "bit" => DataType::Utf8,
        "char" | "varchar" | "text" | "json" => DataType::Utf8,
        "binary" | "varbinary" | "blob" => DataType::Binary,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "datetime" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "date" => DataType::Date32,
        "time" => DataType::Time64(TimeUnit::Microsecond),
        _ => unimplemented!("Unsupported data type: {}", mysql_dtype),
    }
}
