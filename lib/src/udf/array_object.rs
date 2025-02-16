// udf/array_object.rs: User defined scalar function.

use std::sync::Arc;

use datafusion::arrow::datatypes::Fields;
use datafusion::{
    arrow::{
        array::{ListArray, StructArray},
        datatypes::{DataType, Field},
    },
    error::DataFusionError,
    logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility},
    scalar::ScalarValue,
};

#[allow(clippy::module_name_repetitions)]
pub fn array_object_udf() -> ScalarUDF {
    let array_object_fn = Arc::new(|args: &[ColumnarValue]| {
        assert_eq!(args.len(), 3);

        if let (
            ColumnarValue::Array(array),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(index))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(field_name))),
        ) = (&args[0], &args[1], &args[2])
        {
            let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
            let struct_value = list_array.value(usize::try_from(*index).unwrap());
            let struct_array = struct_value.as_any().downcast_ref::<StructArray>().unwrap();

            if let Some((field_index, _)) = struct_array.fields().find(field_name) {
                let field_value = struct_array.column(field_index);
                Ok(ColumnarValue::Array(field_value.clone()))
            } else {
                Err(DataFusionError::Execution(format!(
                    "Field '{field_name}' not found in struct",
                )))
            }
        } else {
            Err(DataFusionError::Execution(
                "Invalid argument types".to_string(),
            ))
        }
    });

    create_udf(
        "array_object",
        vec![
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(Fields::empty()), // Empty vector means dynamic fields
                true,
            ))),
            DataType::Int64,
            DataType::Utf8,
        ],
        DataType::Utf8.into(), // TODO: Assuming the field value is a string, adjust as needed
        Volatility::Immutable,
        array_object_fn,
    )
}
