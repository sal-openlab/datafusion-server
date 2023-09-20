// schema.rs - Schema of data sources
// Sasaki, Naoki <nsasaki@sal.co.jp> January 29, 2023
//

use datafusion::arrow;
use datafusion::arrow::datatypes::{SchemaRef, TimeUnit};
use serde::Deserialize;
use serde_derive::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum DataType {
    Unknown,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Integer, // alias as Int64
    Float16,
    Float32,
    Float64,
    Float,     // alias as Float64
    Timestamp, // counting the milliseconds from 00:00:00 on 1 January 1970 as UTC
    TimestampSecond,
    TimestampMicro,
    TimestampNano,
    Date,     // elapsed time since 00:00:00.000 on 1 January 1970 in milliseconds
    Time,     // elapsed time since midnight in milliseconds
    Duration, // measure of elapsed time in milliseconds
    DurationSecond,
    DurationMicro,
    DurationNano,
    String, // variable length string in Unicode with UTF-8 encoding
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Field {
    pub name: String,
    #[serde(rename = "dataType")]
    pub data_type: DataType,
    pub nullable: Option<bool>,
}

impl Field {
    pub fn from_datafusion_field(field: &arrow::datatypes::Field) -> Self {
        Self {
            name: field.name().clone(),
            data_type: Self::datafusion_data_type(field.data_type()),
            nullable: Some(field.is_nullable()),
        }
    }

    fn arrow_data_type(&self) -> arrow::datatypes::DataType {
        match self.data_type {
            DataType::Boolean => arrow::datatypes::DataType::Boolean,
            DataType::Int8 => arrow::datatypes::DataType::Int8,
            DataType::Int16 => arrow::datatypes::DataType::Int16,
            DataType::Int32 => arrow::datatypes::DataType::Int32,
            DataType::Int64 | DataType::Integer => arrow::datatypes::DataType::Int64,
            DataType::UInt8 => arrow::datatypes::DataType::UInt8,
            DataType::UInt16 => arrow::datatypes::DataType::UInt16,
            DataType::UInt32 => arrow::datatypes::DataType::UInt32,
            DataType::UInt64 => arrow::datatypes::DataType::UInt64,
            DataType::Float16 => arrow::datatypes::DataType::Float16,
            DataType::Float32 => arrow::datatypes::DataType::Float32,
            DataType::Float64 | DataType::Float => arrow::datatypes::DataType::Float64,
            DataType::Timestamp => {
                arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, None)
            }
            DataType::TimestampSecond => {
                arrow::datatypes::DataType::Timestamp(TimeUnit::Second, None)
            }
            DataType::TimestampMicro => {
                arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, None)
            }
            DataType::TimestampNano => {
                arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, None)
            }
            DataType::Date => arrow::datatypes::DataType::Date64,
            DataType::Time => arrow::datatypes::DataType::Time32(TimeUnit::Millisecond),
            DataType::Duration => arrow::datatypes::DataType::Duration(TimeUnit::Millisecond),
            DataType::DurationSecond => arrow::datatypes::DataType::Duration(TimeUnit::Second),
            DataType::DurationMicro => arrow::datatypes::DataType::Duration(TimeUnit::Microsecond),
            DataType::DurationNano => arrow::datatypes::DataType::Duration(TimeUnit::Nanosecond),
            DataType::String => arrow::datatypes::DataType::Utf8,
            DataType::Unknown => arrow::datatypes::DataType::Binary,
        }
    }

    fn datafusion_data_type(data_type: &arrow::datatypes::DataType) -> DataType {
        match data_type {
            arrow::datatypes::DataType::Boolean => DataType::Boolean,
            arrow::datatypes::DataType::Int8 => DataType::Int8,
            arrow::datatypes::DataType::Int16 => DataType::Int16,
            arrow::datatypes::DataType::Int32 => DataType::Int32,
            arrow::datatypes::DataType::Int64 => DataType::Int64,
            arrow::datatypes::DataType::UInt8 => DataType::UInt8,
            arrow::datatypes::DataType::UInt16 => DataType::UInt16,
            arrow::datatypes::DataType::UInt32 => DataType::UInt32,
            arrow::datatypes::DataType::UInt64 => DataType::UInt64,
            arrow::datatypes::DataType::Float16 => DataType::Float16,
            arrow::datatypes::DataType::Float32 => DataType::Float32,
            arrow::datatypes::DataType::Float64 => DataType::Float64,
            arrow::datatypes::DataType::Timestamp(TimeUnit::Millisecond, None) => {
                DataType::Timestamp
            }
            arrow::datatypes::DataType::Timestamp(TimeUnit::Second, None) => {
                DataType::TimestampSecond
            }
            arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, None) => {
                DataType::TimestampMicro
            }
            arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                DataType::TimestampNano
            }
            arrow::datatypes::DataType::Date32 | arrow::datatypes::DataType::Date64 => {
                DataType::Date
            }
            arrow::datatypes::DataType::Time32(TimeUnit::Millisecond) => DataType::Time,
            arrow::datatypes::DataType::Duration(TimeUnit::Millisecond) => DataType::Duration,
            arrow::datatypes::DataType::Duration(TimeUnit::Microsecond) => DataType::DurationMicro,
            arrow::datatypes::DataType::Duration(TimeUnit::Nanosecond) => DataType::DurationNano,
            arrow::datatypes::DataType::Utf8 => DataType::String,
            _ => DataType::Unknown,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(transparent)]
#[allow(clippy::module_name_repetitions)]
pub struct DataSourceSchema {
    pub fields: Vec<Field>,
}

impl DataSourceSchema {
    pub fn to_datafusion_schema(&self) -> arrow::datatypes::Schema {
        let mut schema_fields = Vec::<arrow::datatypes::Field>::new();

        for field in &self.fields {
            schema_fields.push(arrow::datatypes::Field::new(
                field.name.clone(),
                field.arrow_data_type(),
                field.nullable.unwrap_or(true),
            ));
        }

        arrow::datatypes::Schema::new(schema_fields)
    }

    pub fn from_datafusion_schema(schema: &SchemaRef) -> Self {
        let mut fields = Vec::<Field>::new();

        for field in &schema.fields {
            fields.push(Field::from_datafusion_field(field));
        }

        Self { fields }
    }
}
