// scalar_value.rs - Scalar value for variable of session context.
// Sasaki, Naoki <nsasaki@sal.co.jp> May 24, 2025
//

use std::collections::HashMap;

use datafusion::{error::DataFusionError, scalar::ScalarValue, variable::VarProvider};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::data_source::data_type::DataType;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SessionVariable {
    pub name: String,
    #[serde(rename = "dataType")]
    pub data_type: DataType,
    pub value: serde_json::Value,
}

impl SessionVariable {
    pub fn to_scalar_value(&self) -> Option<ScalarValue> {
        #[allow(clippy::cast_possible_truncation)]
        match self.data_type {
            DataType::Boolean => Some(ScalarValue::Boolean(match self.value {
                JsonValue::Null => None,
                JsonValue::Bool(b) => Some(b),
                _ => return None,
            })),
            DataType::Int8 => Some(ScalarValue::Int8(match &self.value {
                JsonValue::Null => None,
                JsonValue::Number(n) => n.as_i64().map(|v| v as i8),
                _ => return None,
            })),
            DataType::Int16 => Some(ScalarValue::Int16(match &self.value {
                JsonValue::Null => None,
                JsonValue::Number(n) => n.as_i64().map(|v| v as i16),
                _ => return None,
            })),
            DataType::Int32 => Some(ScalarValue::Int32(match &self.value {
                JsonValue::Null => None,
                JsonValue::Number(n) => n.as_i64().map(|v| v as i32),
                _ => return None,
            })),
            DataType::Int64 | DataType::Integer => Some(ScalarValue::Int64(match &self.value {
                JsonValue::Null => None,
                JsonValue::Number(n) => n.as_i64(),
                _ => return None,
            })),
            DataType::UInt8 => Some(ScalarValue::UInt8(match &self.value {
                JsonValue::Null => None,
                JsonValue::Number(n) => n.as_u64().map(|v| v as u8),
                _ => return None,
            })),
            DataType::UInt16 => Some(ScalarValue::UInt16(match &self.value {
                JsonValue::Null => None,
                JsonValue::Number(n) => n.as_u64().map(|v| v as u16),
                _ => return None,
            })),
            DataType::UInt32 => Some(ScalarValue::UInt32(match &self.value {
                JsonValue::Null => None,
                JsonValue::Number(n) => n.as_u64().map(|v| v as u32),
                _ => return None,
            })),
            DataType::UInt64 => Some(ScalarValue::UInt64(match &self.value {
                JsonValue::Null => None,
                JsonValue::Number(n) => n.as_u64(),
                _ => return None,
            })),
            DataType::Float32 => Some(ScalarValue::Float32(match &self.value {
                JsonValue::Null => None,
                JsonValue::Number(n) => n.as_f64().map(|v| v as f32),
                _ => return None,
            })),
            DataType::Float64 | DataType::Float => Some(ScalarValue::Float64(match &self.value {
                JsonValue::Null => None,
                JsonValue::Number(n) => n.as_f64(),
                _ => return None,
            })),
            DataType::String => Some(ScalarValue::Utf8(match &self.value {
                JsonValue::Null => None,
                JsonValue::String(s) => Some(s.clone()),
                n @ JsonValue::Number(_) => Some(n.to_string()),
                b @ JsonValue::Bool(_) => Some(b.to_string()),
                _ => return None,
            })),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct SessionVariableProvider {
    pub(crate) inner: HashMap<String, ScalarValue>,
}

impl VarProvider for SessionVariableProvider {
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue, DataFusionError> {
        log::debug!(">>> SessionVariableProvider::get_value(): {var_names:?}");

        if let Some(name) = var_names.first() {
            match self.inner.get(name) {
                Some(value) => Ok(value.clone()),
                None => Err(DataFusionError::Execution(format!(
                    "Unknown session variable: {name}"
                ))),
            }
        } else {
            Err(DataFusionError::Execution(
                "No variable name given".to_string(),
            ))
        }
    }

    fn get_type(&self, var_names: &[String]) -> Option<datafusion::arrow::datatypes::DataType> {
        var_names
            .first()
            .and_then(|name| self.inner.get(name).map(ScalarValue::data_type))
    }
}
