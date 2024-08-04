// json_with_jsonpath - JSON UTF8 text to RecordBatch transform by JSONPath
// Sasaki, Naoki <nsasaki@sal.co.jp> January 9, 2023
//

use datafusion::arrow::{self, record_batch::RecordBatch};
use jsonpath_rust::{JsonPath, JsonPathValue};
use serde_json::Value;

use crate::data_source::{decoder::json_decoder, infer_schema, schema::DataSourceSchema};
use crate::request::body::DataSourceOption;
use crate::response::http_error::ResponseError;

pub fn to_record_batch(
    utf8text: &str,
    schema: &Option<DataSourceSchema>,
    options: &DataSourceOption,
) -> Result<Vec<RecordBatch>, ResponseError> {
    let json_path = match &options.json_path {
        Some(o) => o.as_str(),
        None => "$.*",
    };

    let json = serde_json::from_str(utf8text)?;
    let path_finder = JsonPath::try_from(json_path)
        .map_err(|e| ResponseError::json_parsing(format!("Invalid JSONPath {json_path:?}: {e}")))?;

    let found_slices: Vec<JsonPathValue<Value>> = path_finder.find_slice(&json);
    let json_rows: Vec<Value> = found_slices
        .into_iter()
        .map(JsonPathValue::to_data)
        .collect();

    let df_schema = if let Some(schema) = schema {
        schema.to_arrow_schema()
    } else {
        infer_schema::from_json_value(&json_rows, options)?
    };

    log::debug!("number of parsed JSON objects: {}", json_rows.len());
    log::debug!("detected: {:?}", df_schema);

    let mut record_batches = Vec::<RecordBatch>::new();
    {
        let mut values: Box<dyn Iterator<Item = arrow::error::Result<Value>>> = if options
            .require_normalize
            .unwrap_or(false)
        {
            Box::new(json_rows.iter().map(|json_row| {
                let mut map = serde_json::map::Map::new();
                df_schema.fields().iter().enumerate().try_for_each(
                    |(index, field)| match json_row.get(index) {
                        Some(value) => {
                            map.insert(field.name().to_string(), value.clone());
                            Ok(())
                        }
                        // TODO: fills-up default value when normalize is required
                        None => Err(arrow::error::ArrowError::JsonError(format!(
                            "missing JSON column {index} in row {json_row}"
                        ))),
                    },
                )?;
                Ok(Value::Object(map))
            }))
        } else {
            Box::new(json_rows.clone().into_iter().map(Ok))
        };

        let decoder = json_decoder::Decoder::new(
            arrow::datatypes::SchemaRef::new(df_schema.clone()),
            json_decoder::DecoderOptions::new(),
        );

        while let Some(batch) = decoder
            .next_batch(&mut values)
            .map_err(ResponseError::record_batch_extraction)?
        {
            record_batches.push(batch);
        }
    }

    Ok(record_batches)
}
