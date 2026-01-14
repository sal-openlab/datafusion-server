// schema.rs - Schema of data sources
// Sasaki, Naoki <nsasaki@sal.co.jp> January 29, 2023
//

use arrow::error::ArrowError;
use datafusion::arrow::{self, datatypes::SchemaRef};
use serde::Deserialize;
use serde_derive::Serialize;
use std::sync::Arc;

use crate::data_source::data_type::DataType;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Field {
    pub name: String,
    #[serde(rename = "dataType")]
    pub data_type: DataType,
    pub nullable: Option<bool>,
}

impl Field {
    #[allow(dead_code)]
    fn to_arrow_field(&self) -> Result<Arc<arrow::datatypes::Field>, ArrowError> {
        Ok(Arc::new(arrow::datatypes::Field::new(
            self.name.clone(),
            self.data_type.to_arrow_data_type()?,
            self.nullable.unwrap_or(true),
        )))
    }

    pub fn from_arrow_field(field: &arrow::datatypes::Field) -> Self {
        Self {
            name: field.name().clone(),
            data_type: DataType::from_arrow_data_type(field.data_type()),
            nullable: Some(field.is_nullable()),
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
    pub fn to_arrow_schema(&self) -> Result<arrow::datatypes::Schema, ArrowError> {
        let mut schema_fields = Vec::<arrow::datatypes::Field>::new();

        for field in &self.fields {
            schema_fields.push(arrow::datatypes::Field::new(
                field.name.clone(),
                field.data_type.to_arrow_data_type()?,
                field.nullable.unwrap_or(true),
            ));
        }

        Ok(arrow::datatypes::Schema::new(schema_fields))
    }

    pub fn from_arrow_schema(schema: &SchemaRef) -> Self {
        let mut fields = Vec::<Field>::new();

        for field in &schema.fields {
            fields.push(Field::from_arrow_field(field));
        }

        Self { fields }
    }
}
