// json_decoder.rs - Decode to RecordBatch from native JSON
// Sasaki, Naoki <nsasaki@sal.co.jp> March 28, 2023
//

use datafusion::arrow::{
    array::{
        make_array, Array, ArrayBuilder, ArrayData, ArrayDataBuilder, ArrayRef, BinaryArray,
        BooleanBuilder, Decimal128Array, Decimal256Array, GenericListArray, LargeStringArray,
        ListBuilder, NullArray, OffsetSizeTrait, PrimitiveArray, StringArray, StringBuilder,
        StringDictionaryBuilder,
    },
    buffer::{Buffer, MutableBuffer},
    compute::kernels::cast_utils::Parser,
    datatypes::{
        i256, ArrowDictionaryKeyType, ArrowNativeType, ArrowNativeTypeOp, ArrowPrimitiveType,
        DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, DecimalType, Field,
        FieldRef, Fields, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
        TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchOptions},
    util::bit_util,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Options for native JSON decoding
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::module_name_repetitions)]
pub struct DecoderOptions {
    /// Batch size (number of records to load each time), defaults to 1024 records
    batch_size: usize,
    /// Optional projection for which columns to load (case-sensitive names)
    projection: Option<Vec<String>>,
    /// optional HashMap of column name to its format string
    format_strings: Option<HashMap<String, String>>,
}

impl Default for DecoderOptions {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            projection: None,
            format_strings: None,
        }
    }
}

impl DecoderOptions {
    pub fn new() -> Self {
        DecoderOptions::default()
    }

    #[allow(dead_code)]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    #[allow(dead_code)]
    pub fn with_projection(mut self, projection: Vec<String>) -> Self {
        self.projection = Some(projection);
        self
    }

    #[allow(dead_code)]
    pub fn with_format_strings(mut self, format_strings: HashMap<String, String>) -> Self {
        self.format_strings = Some(format_strings);
        self
    }
}

#[derive(Debug)]
pub struct Decoder {
    schema: SchemaRef,
    options: DecoderOptions,
}

impl Decoder {
    pub fn new(schema: SchemaRef, options: DecoderOptions) -> Self {
        Self { schema, options }
    }

    pub fn next_batch<I>(&self, value_iter: &mut I) -> Result<Option<RecordBatch>, ArrowError>
    where
        I: Iterator<Item = Result<Value, ArrowError>>,
    {
        let batch_size = self.options.batch_size;
        let mut rows: Vec<Value> = Vec::with_capacity(batch_size);

        for value in value_iter.by_ref().take(batch_size) {
            let v = value?;
            match v {
                Value::Object(_) => rows.push(v),
                _ => {
                    return Err(ArrowError::JsonError(format!(
                        "Row needs to be of type object, got: {v:?}"
                    )));
                }
            }
        }

        if rows.is_empty() {
            // reached end of file
            return Ok(None);
        }

        let rows = &rows[..];
        let arrays = self.build_struct_array(rows, self.schema.fields(), &self.options.projection);

        let projected_fields = if let Some(projection) = self.options.projection.as_ref() {
            projection
                .iter()
                .filter_map(|name| self.schema.column_with_name(name))
                .map(|(_, field)| field.clone())
                .collect()
        } else {
            self.schema.fields().clone()
        };

        let projected_schema = Arc::new(Schema::new(projected_fields));

        arrays.and_then(|arr| {
            RecordBatch::try_new_with_options(
                projected_schema,
                arr,
                &RecordBatchOptions::new()
                    .with_match_field_names(true)
                    .with_row_count(Some(rows.len())),
            )
            .map(Some)
        })
    }

    #[allow(clippy::too_many_lines)]
    fn build_struct_array(
        &self,
        rows: &[Value],
        struct_fields: &Fields,
        projection: &Option<Vec<String>>,
    ) -> Result<Vec<ArrayRef>, ArrowError> {
        let arrays: Result<Vec<ArrayRef>, ArrowError> = struct_fields
            .iter()
            .filter(|field| {
                #[allow(clippy::map_unwrap_or)]
                projection
                    .as_ref()
                    .map(|p| p.contains(field.name()))
                    .unwrap_or(true)
            })
            .map(|field| {
                match field.data_type() {
                    DataType::Null => Ok(Arc::new(NullArray::new(rows.len())) as ArrayRef),
                    DataType::Boolean => self.build_boolean_array(rows, field.name()),
                    DataType::Float64 => {
                        self.build_primitive_array::<Float64Type>(rows, field.name())
                    }
                    DataType::Float32 => {
                        self.build_primitive_array::<Float32Type>(rows, field.name())
                    }
                    DataType::Int64 => self.build_primitive_array::<Int64Type>(rows, field.name()),
                    DataType::Int32 => self.build_primitive_array::<Int32Type>(rows, field.name()),
                    DataType::Int16 => self.build_primitive_array::<Int16Type>(rows, field.name()),
                    DataType::Int8 => self.build_primitive_array::<Int8Type>(rows, field.name()),
                    DataType::UInt64 => {
                        self.build_primitive_array::<UInt64Type>(rows, field.name())
                    }
                    DataType::UInt32 => {
                        self.build_primitive_array::<UInt32Type>(rows, field.name())
                    }
                    DataType::UInt16 => {
                        self.build_primitive_array::<UInt16Type>(rows, field.name())
                    }
                    DataType::UInt8 => self.build_primitive_array::<UInt8Type>(rows, field.name()),
                    // TODO: this is incomplete
                    DataType::Timestamp(unit, _) => match unit {
                        TimeUnit::Second => {
                            self.build_primitive_array::<TimestampSecondType>(rows, field.name())
                        }
                        TimeUnit::Microsecond => self
                            .build_primitive_array::<TimestampMicrosecondType>(rows, field.name()),
                        TimeUnit::Millisecond => self
                            .build_primitive_array::<TimestampMillisecondType>(rows, field.name()),
                        TimeUnit::Nanosecond => self
                            .build_primitive_array::<TimestampNanosecondType>(rows, field.name()),
                    },
                    DataType::Date64 => {
                        self.build_primitive_array::<Date64Type>(rows, field.name())
                    }
                    DataType::Date32 => {
                        self.build_primitive_array::<Date32Type>(rows, field.name())
                    }
                    DataType::Time64(unit) => {
                        match unit {
                            TimeUnit::Microsecond => self
                                .build_primitive_array::<Time64MicrosecondType>(rows, field.name()),
                            TimeUnit::Nanosecond => self
                                .build_primitive_array::<Time64NanosecondType>(rows, field.name()),
                            t => Err(ArrowError::JsonError(format!(
                                "TimeUnit {t:?} not supported with Time64"
                            ))),
                        }
                    }
                    DataType::Time32(unit) => match unit {
                        TimeUnit::Second => {
                            self.build_primitive_array::<Time32SecondType>(rows, field.name())
                        }
                        TimeUnit::Millisecond => {
                            self.build_primitive_array::<Time32MillisecondType>(rows, field.name())
                        }
                        t => Err(ArrowError::JsonError(format!(
                            "TimeUnit {t:?} not supported with Time32"
                        ))),
                    },
                    DataType::Utf8 => Ok(Arc::new(
                        rows.iter()
                            .map(|row| {
                                let maybe_value = row.get(field.name());
                                maybe_value.and_then(Value::as_str)
                            })
                            .collect::<StringArray>(),
                    ) as ArrayRef),
                    DataType::Binary => Ok(Arc::new(
                        rows.iter()
                            .map(|row| {
                                let maybe_value = row.get(field.name());
                                maybe_value.and_then(Value::as_str)
                            })
                            .collect::<BinaryArray>(),
                    ) as ArrayRef),
                    DataType::List(ref list_field) => {
                        if let DataType::Dictionary(ref key_ty, _) = list_field.data_type() {
                            self.build_wrapped_list_array(rows, field.name(), key_ty)
                        } else {
                            let extracted_rows = rows
                                .iter()
                                .map(|row| row.get(field.name()).cloned().unwrap_or(Value::Null))
                                .collect::<Vec<Value>>();
                            self.build_nested_list_array::<i32>(
                                extracted_rows.as_slice(),
                                list_field,
                            )
                        }
                    }
                    DataType::Dictionary(ref key_ty, ref val_ty) => {
                        self.build_string_dictionary_array(rows, field.name(), key_ty, val_ty)
                    }
                    DataType::Struct(fields) => {
                        let len = rows.len();
                        let num_bytes = bit_util::ceil(len, 8);
                        let mut null_buffer = MutableBuffer::from_len_zeroed(num_bytes);
                        let struct_rows = rows
                            .iter()
                            .enumerate()
                            .map(|(i, row)| (i, row.as_object().and_then(|v| v.get(field.name()))))
                            .map(|(i, v)| match v {
                                // we want the field as an object, if it's not, we treat as null
                                Some(Value::Object(value)) => {
                                    bit_util::set_bit(null_buffer.as_slice_mut(), i);
                                    Value::Object(value.clone())
                                }
                                _ => Value::Object(serde_json::Map::default()),
                            })
                            .collect::<Vec<Value>>();
                        let arrays = self.build_struct_array(&struct_rows, fields, &None)?;
                        // construct a struct array's data in order to set null buffer
                        let data_type = DataType::Struct(fields.clone());
                        let data = ArrayDataBuilder::new(data_type)
                            .len(len)
                            .null_bit_buffer(Some(null_buffer.into()))
                            .child_data(
                                arrays
                                    .into_iter()
                                    .map(datafusion::arrow::array::Array::into_data)
                                    .collect(),
                            );
                        let data = unsafe { data.build_unchecked() };
                        Ok(make_array(data))
                    }
                    DataType::Map(map_field, _) => {
                        self.build_map_array(rows, field.name(), field.data_type(), map_field)
                    }
                    DataType::Decimal128(precision, scale) => {
                        self.build_decimal128_array(rows, field.name(), *precision, *scale)
                    }
                    DataType::Decimal256(precision, scale) => {
                        self.build_decimal256_array(rows, field.name(), *precision, *scale)
                    }
                    _ => Err(ArrowError::JsonError(format!(
                        "{:?} type is not supported",
                        field.data_type()
                    ))),
                }
            })
            .collect();
        arrays
    }

    fn build_map_array(
        &self,
        rows: &[Value],
        field_name: &str,
        map_type: &DataType,
        struct_field: &Field,
    ) -> Result<ArrayRef, ArrowError> {
        // A map has the format {"key": "value"} where key is most commonly a string,
        // but could be a string, number or boolean (e.g. {1: "value"}).
        // A map is also represented as a flattened contiguous array, with the number
        // of key-value pairs being separated by a list offset.
        // If row 1 has 2 key-value pairs, and row 2 has 3, the offsets would be
        // [0, 2, 5].
        //
        // Thus we try to read a map by iterating through the keys and values

        let (key_field, value_field) = if let DataType::Struct(fields) = struct_field.data_type() {
            if fields.len() != 2 {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "DataType::Map expects a struct with 2 fields, found {} fields",
                    fields.len()
                )));
            }
            (&fields[0], &fields[1])
        } else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "JSON map array builder expects a DataType::Map, found {:?}",
                struct_field.data_type()
            )));
        };
        let value_map_iter = rows.iter().map(|value| {
            #[allow(clippy::cast_possible_truncation)]
            #[allow(clippy::cast_possible_wrap)]
            value
                .get(field_name)
                .and_then(|v| v.as_object().map(|map| (map, map.len() as i32)))
        });
        let rows_len = rows.len();
        let mut list_offsets = Vec::with_capacity(rows_len + 1);
        list_offsets.push(0i32);
        let mut last_offset = 0;
        let num_bytes = bit_util::ceil(rows_len, 8);
        let mut list_bitmap = MutableBuffer::from_len_zeroed(num_bytes);
        let null_data = list_bitmap.as_slice_mut();

        let struct_rows = value_map_iter
            .enumerate()
            .filter_map(|(i, v)| {
                if let Some((map, len)) = v {
                    list_offsets.push(last_offset + len);
                    last_offset += len;
                    bit_util::set_bit(null_data, i);
                    Some(map.iter().map(|(k, v)| {
                        json!({
                            key_field.name(): k,
                            value_field.name(): v
                        })
                    }))
                } else {
                    list_offsets.push(last_offset);
                    None
                }
            })
            .flatten()
            .collect::<Vec<Value>>();

        let struct_children = self.build_struct_array(
            struct_rows.as_slice(),
            &Fields::from([key_field.clone(), value_field.clone()]),
            &None,
        )?;

        unsafe {
            Ok(make_array(ArrayData::new_unchecked(
                map_type.clone(),
                rows_len,
                None,
                Some(list_bitmap.into()),
                0,
                vec![Buffer::from_slice_ref(&list_offsets)],
                vec![ArrayData::new_unchecked(
                    struct_field.data_type().clone(),
                    struct_children[0].len(),
                    None,
                    None,
                    0,
                    vec![],
                    struct_children
                        .into_iter()
                        .map(datafusion::arrow::array::Array::into_data)
                        .collect(),
                )],
            )))
        }
    }

    #[allow(clippy::too_many_lines)]
    fn build_nested_list_array<OffsetSize: OffsetSizeTrait>(
        &self,
        rows: &[Value],
        list_field: &FieldRef,
    ) -> Result<ArrayRef, ArrowError> {
        // build list offsets
        let mut cur_offset = OffsetSize::zero();
        let list_len = rows.len();
        let num_list_bytes = bit_util::ceil(list_len, 8);
        let mut offsets = Vec::with_capacity(list_len + 1);
        let mut list_nulls = MutableBuffer::from_len_zeroed(num_list_bytes);
        let list_nulls = list_nulls.as_slice_mut();
        offsets.push(cur_offset);
        rows.iter().enumerate().for_each(|(i, v)| {
            if let Value::Array(a) = v {
                cur_offset += OffsetSize::from_usize(a.len()).unwrap();
                bit_util::set_bit(list_nulls, i);
            } else if let Value::Null = v {
                // value is null, not incremented
            } else {
                cur_offset += OffsetSize::one();
            }
            offsets.push(cur_offset);
        });
        let valid_len = cur_offset.to_usize().unwrap();
        let array_data = match list_field.data_type() {
            DataType::Null => NullArray::new(valid_len).into_data(),
            DataType::Boolean => {
                let num_bytes = bit_util::ceil(valid_len, 8);
                let mut bool_values = MutableBuffer::from_len_zeroed(num_bytes);
                let mut bool_nulls = MutableBuffer::new(num_bytes).with_bitset(num_bytes, true);
                let mut curr_index = 0;
                for v in rows {
                    if let Value::Array(vs) = v {
                        for value in vs {
                            if let Value::Bool(child) = value {
                                // if valid boolean, append value
                                if *child {
                                    bit_util::set_bit(bool_values.as_slice_mut(), curr_index);
                                }
                            } else {
                                // null slot
                                bit_util::unset_bit(bool_nulls.as_slice_mut(), curr_index);
                            }
                            curr_index += 1;
                        }
                    }
                }
                unsafe {
                    ArrayData::builder(list_field.data_type().clone())
                        .len(valid_len)
                        .add_buffer(bool_values.into())
                        .null_bit_buffer(Some(bool_nulls.into()))
                        .build_unchecked()
                }
            }
            DataType::Int8 => self.read_primitive_list_values::<Int8Type>(rows),
            DataType::Int16 => self.read_primitive_list_values::<Int16Type>(rows),
            DataType::Int32 => self.read_primitive_list_values::<Int32Type>(rows),
            DataType::Int64 => self.read_primitive_list_values::<Int64Type>(rows),
            DataType::UInt8 => self.read_primitive_list_values::<UInt8Type>(rows),
            DataType::UInt16 => self.read_primitive_list_values::<UInt16Type>(rows),
            DataType::UInt32 => self.read_primitive_list_values::<UInt32Type>(rows),
            DataType::UInt64 => self.read_primitive_list_values::<UInt64Type>(rows),
            DataType::Float16 => {
                return Err(ArrowError::JsonError("Float16 not supported".to_string()))
            }
            DataType::Float32 => self.read_primitive_list_values::<Float32Type>(rows),
            DataType::Float64 => self.read_primitive_list_values::<Float64Type>(rows),
            DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_) => {
                return Err(ArrowError::JsonError(
                    "Temporal types are not yet supported, see ARROW-4803".to_string(),
                ))
            }
            DataType::Utf8 => flatten_json_string_values(rows)
                .into_iter()
                .collect::<StringArray>()
                .into_data(),
            DataType::LargeUtf8 => flatten_json_string_values(rows)
                .into_iter()
                .collect::<LargeStringArray>()
                .into_data(),
            DataType::List(field) => {
                let child =
                    self.build_nested_list_array::<i32>(&flatten_json_values(rows), field)?;
                child.into_data()
            }
            DataType::LargeList(field) => {
                let child =
                    self.build_nested_list_array::<i64>(&flatten_json_values(rows), field)?;
                child.into_data()
            }
            DataType::Struct(fields) => {
                // extract list values, with non-lists converted to Value::Null
                let array_item_count = cur_offset.to_usize().unwrap();
                let num_bytes = bit_util::ceil(array_item_count, 8);
                let mut null_buffer = MutableBuffer::from_len_zeroed(num_bytes);
                let mut struct_index = 0;
                let rows: Vec<Value> = rows
                    .iter()
                    .flat_map(|row| match row {
                        Value::Array(values) if !values.is_empty() => {
                            for value in values {
                                if !value.is_null() {
                                    bit_util::set_bit(null_buffer.as_slice_mut(), struct_index);
                                }
                                struct_index += 1;
                            }
                            values.clone()
                        }
                        _ => {
                            vec![]
                        }
                    })
                    .collect();
                let arrays = self.build_struct_array(rows.as_slice(), fields, &None)?;
                let data_type = DataType::Struct(fields.clone());
                let buf = null_buffer.into();
                unsafe {
                    ArrayDataBuilder::new(data_type)
                        .len(rows.len())
                        .null_bit_buffer(Some(buf))
                        .child_data(
                            arrays
                                .into_iter()
                                .map(datafusion::arrow::array::Array::into_data)
                                .collect(),
                        )
                        .build_unchecked()
                }
            }
            datatype => {
                return Err(ArrowError::JsonError(format!(
                    "Nested list of {datatype:?} not supported"
                )));
            }
        };
        // build list
        let list_data = ArrayData::builder(DataType::List(list_field.clone()))
            .len(list_len)
            .add_buffer(Buffer::from_slice_ref(&offsets))
            .add_child_data(array_data)
            .null_bit_buffer(Some(list_nulls.into()));
        let list_data = unsafe { list_data.build_unchecked() };
        Ok(Arc::new(GenericListArray::<OffsetSize>::from(list_data)))
    }

    #[allow(clippy::unused_self)]
    fn read_primitive_list_values<T>(&self, rows: &[Value]) -> ArrayData
    where
        T: ArrowPrimitiveType,
        T::Native: num_traits::NumCast,
    {
        let values = rows
            .iter()
            .flat_map(|row| {
                // read values from list
                if let Value::Array(values) = row {
                    values
                        .iter()
                        .map(|value| {
                            let v: Option<T::Native> =
                                value.as_f64().and_then(num_traits::cast::cast);
                            v
                        })
                        .collect::<Vec<Option<T::Native>>>()
                } else if let Value::Number(value) = row {
                    // handle the scalar number case
                    let v: Option<T::Native> = value.as_f64().and_then(num_traits::cast::cast);
                    v.map(|v| vec![Some(v)]).unwrap_or_default()
                } else {
                    vec![]
                }
            })
            .collect::<Vec<Option<T::Native>>>();
        let array = values.iter().collect::<PrimitiveArray<T>>();
        array.into_data()
    }

    #[allow(clippy::unnecessary_wraps)] // require to match `Result` wrapping with other builder functions
    #[allow(clippy::trait_duplication_in_bounds)]
    fn build_primitive_array<T: ArrowPrimitiveType + Parser>(
        &self,
        rows: &[Value],
        col_name: &str,
    ) -> Result<ArrayRef, ArrowError>
    where
        T::Native: num_traits::NumCast,
    {
        let format_string = self
            .options
            .format_strings
            .as_ref()
            .and_then(|fmts| fmts.get(col_name));
        Ok(Arc::new(
            rows.iter()
                .map(|row| {
                    row.get(col_name).and_then(|value| {
                        if value.is_i64() {
                            value.as_i64().and_then(num_traits::cast::cast)
                        } else if value.is_u64() {
                            value.as_u64().and_then(num_traits::cast::cast)
                        } else if value.is_string() {
                            match format_string {
                                Some(fmt) => T::parse_formatted(value.as_str().unwrap(), fmt),
                                None => T::parse(value.as_str().unwrap()),
                            }
                        } else {
                            value.as_f64().and_then(num_traits::cast::cast)
                        }
                    })
                })
                .collect::<PrimitiveArray<T>>(),
        ))
    }

    #[allow(clippy::unnecessary_wraps)] // require to match `Result` wrapping with other builder functions
    #[allow(clippy::unused_self)]
    fn build_boolean_array(&self, rows: &[Value], col_name: &str) -> Result<ArrayRef, ArrowError> {
        let mut builder = BooleanBuilder::with_capacity(rows.len());
        for row in rows {
            if let Some(value) = row.get(col_name) {
                if let Some(boolean) = value.as_bool() {
                    builder.append_value(boolean);
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()))
    }

    #[allow(clippy::unused_self)]
    fn build_decimal128_array(
        &self,
        rows: &[Value],
        col_name: &str,
        precision: u8,
        scale: i8,
    ) -> Result<ArrayRef, ArrowError> {
        Ok(Arc::new(
            rows.iter()
                .map(|row| {
                    row.get(col_name).and_then(|value| {
                        if value.is_i64() {
                            #[allow(clippy::cast_sign_loss)]
                            let mul = 10i128.pow(scale as _);
                            value
                                .as_i64()
                                .and_then(num_traits::cast::cast)
                                .map(|v: i128| v * mul)
                        } else if value.is_u64() {
                            #[allow(clippy::cast_sign_loss)]
                            let mul = 10i128.pow(scale as _);
                            value
                                .as_u64()
                                .and_then(num_traits::cast::cast)
                                .map(|v: i128| v * mul)
                        } else if value.is_string() {
                            value.as_str().and_then(|s| {
                                parse_decimal::<Decimal128Type>(s, precision, scale).ok()
                            })
                        } else {
                            let mul = 10_f64.powi(i32::from(scale));
                            #[allow(clippy::cast_possible_truncation)]
                            value.as_f64().map(|f| (f * mul).round() as i128)
                        }
                    })
                })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(precision, scale)?,
        ))
    }

    #[allow(clippy::unused_self)]
    fn build_decimal256_array(
        &self,
        rows: &[Value],
        col_name: &str,
        precision: u8,
        scale: i8,
    ) -> Result<ArrayRef, ArrowError> {
        #[allow(clippy::cast_lossless)]
        let mul = 10_f64.powi(scale as i32);
        Ok(Arc::new(
            rows.iter()
                .map(|row| {
                    row.get(col_name).and_then(|value| {
                        if value.is_i64() {
                            #[allow(clippy::cast_sign_loss)]
                            let mul = i256::from_i128(10).pow_wrapping(scale as _);
                            value.as_i64().map(|i| i256::from_i128(i128::from(i)) * mul)
                        } else if value.is_u64() {
                            #[allow(clippy::cast_sign_loss)]
                            let mul = i256::from_i128(10).pow_wrapping(scale as _);
                            value.as_u64().map(|i| i256::from_i128(i128::from(i)) * mul)
                        } else if value.is_string() {
                            value.as_str().and_then(|s| {
                                parse_decimal::<Decimal256Type>(s, precision, scale).ok()
                            })
                        } else {
                            value.as_f64().and_then(|f| i256::from_f64(f * mul.round()))
                        }
                    })
                })
                .collect::<Decimal256Array>()
                .with_precision_and_scale(precision, scale)?,
        ))
    }

    fn build_wrapped_list_array(
        &self,
        rows: &[Value],
        col_name: &str,
        key_type: &DataType,
    ) -> Result<ArrayRef, ArrowError> {
        match *key_type {
            DataType::Int8 => {
                let d_type =
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<Int8Type>(&d_type, col_name, rows)
            }
            DataType::Int16 => {
                let d_type =
                    DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<Int16Type>(&d_type, col_name, rows)
            }
            DataType::Int32 => {
                let d_type =
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<Int32Type>(&d_type, col_name, rows)
            }
            DataType::Int64 => {
                let d_type =
                    DataType::Dictionary(Box::new(DataType::Int64), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<Int64Type>(&d_type, col_name, rows)
            }
            DataType::UInt8 => {
                let d_type =
                    DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<UInt8Type>(&d_type, col_name, rows)
            }
            DataType::UInt16 => {
                let d_type =
                    DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<UInt16Type>(&d_type, col_name, rows)
            }
            DataType::UInt32 => {
                let d_type =
                    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<UInt32Type>(&d_type, col_name, rows)
            }
            DataType::UInt64 => {
                let d_type =
                    DataType::Dictionary(Box::new(DataType::UInt64), Box::new(DataType::Utf8));
                self.list_array_string_array_builder::<UInt64Type>(&d_type, col_name, rows)
            }
            ref e => Err(ArrowError::JsonError(format!(
                "Data type is currently not supported for dictionaries in list : {e:?}"
            ))),
        }
    }

    fn list_array_string_array_builder<DT>(
        &self,
        data_type: &DataType,
        col_name: &str,
        rows: &[Value],
    ) -> Result<ArrayRef, ArrowError>
    where
        DT: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let mut builder: Box<dyn ArrayBuilder> = match data_type {
            DataType::Utf8 => {
                let values_builder = StringBuilder::with_capacity(rows.len(), rows.len() * 5);
                Box::new(ListBuilder::new(values_builder))
            }
            DataType::Dictionary(_, _) => {
                let values_builder = self.build_string_dictionary_builder::<DT>(rows.len() * 5);
                Box::new(ListBuilder::new(values_builder))
            }
            e => {
                return Err(ArrowError::JsonError(format!(
                    "Nested list data builder type is not supported: {e:?}"
                )))
            }
        };

        for row in rows {
            if let Some(value) = row.get(col_name) {
                let vals: Vec<Option<String>> = if let Value::String(v) = value {
                    vec![Some(v.to_string())]
                } else if let Value::Array(n) = value {
                    n.iter()
                        .map(|v: &Value| {
                            if v.is_string() {
                                Some(v.as_str().unwrap().to_string())
                            } else if v.is_array() || v.is_object() || v.is_null() {
                                // implicitly drop nested values
                                // TODO: support deep-nesting
                                None
                            } else {
                                Some(v.to_string())
                            }
                        })
                        .collect()
                } else if let Value::Null = value {
                    vec![None]
                } else if !value.is_object() {
                    vec![Some(value.to_string())]
                } else {
                    return Err(ArrowError::JsonError(
                        "Only scalars are currently supported in JSON arrays".to_string(),
                    ));
                };

                match data_type {
                    DataType::Utf8 => {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<ListBuilder<StringBuilder>>()
                            .ok_or_else(||ArrowError::JsonError(
                                "Cast failed for ListBuilder<StringBuilder> during nested data parsing".to_string(),
                            ))?;
                        for val in vals {
                            if let Some(v) = val {
                                builder.values().append_value(&v);
                            } else {
                                builder.values().append_null();
                            };
                        }

                        builder.append(true);
                    }
                    DataType::Dictionary(_, _) => {
                        let builder = builder.as_any_mut().downcast_mut::<ListBuilder<StringDictionaryBuilder<DT>>>().ok_or_else(||ArrowError::JsonError(
                            "Cast failed for ListBuilder<StringDictionaryBuilder> during nested data parsing".to_string(),
                        ))?;
                        for val in vals {
                            if let Some(v) = val {
                                builder.values().append(&v)?;
                            } else {
                                builder.values().append_null();
                            };
                        }

                        builder.append(true);
                    }
                    e => {
                        return Err(ArrowError::JsonError(format!(
                            "Nested list data builder type is not supported: {e:?}"
                        )))
                    }
                }
            }
        }

        Ok(builder.finish() as ArrayRef)
    }

    #[allow(clippy::unused_self)]
    fn build_string_dictionary_builder<T>(&self, row_len: usize) -> StringDictionaryBuilder<T>
    where
        T: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        StringDictionaryBuilder::with_capacity(row_len, row_len, row_len * 5)
    }

    fn build_string_dictionary_array(
        &self,
        rows: &[Value],
        col_name: &str,
        key_type: &DataType,
        value_type: &DataType,
    ) -> Result<ArrayRef, ArrowError> {
        if let DataType::Utf8 = *value_type {
            match *key_type {
                DataType::Int8 => self.build_dictionary_array::<Int8Type>(rows, col_name),
                DataType::Int16 => self.build_dictionary_array::<Int16Type>(rows, col_name),
                DataType::Int32 => self.build_dictionary_array::<Int32Type>(rows, col_name),
                DataType::Int64 => self.build_dictionary_array::<Int64Type>(rows, col_name),
                DataType::UInt8 => self.build_dictionary_array::<UInt8Type>(rows, col_name),
                DataType::UInt16 => self.build_dictionary_array::<UInt16Type>(rows, col_name),
                DataType::UInt32 => self.build_dictionary_array::<UInt32Type>(rows, col_name),
                DataType::UInt64 => self.build_dictionary_array::<UInt64Type>(rows, col_name),
                _ => Err(ArrowError::JsonError(
                    "unsupported dictionary key type".to_string(),
                )),
            }
        } else {
            Err(ArrowError::JsonError(
                "dictionary types other than UTF-8 not yet supported".to_string(),
            ))
        }
    }

    #[inline]
    fn build_dictionary_array<T>(
        &self,
        rows: &[Value],
        col_name: &str,
    ) -> Result<ArrayRef, ArrowError>
    where
        T::Native: num_traits::NumCast,
        T: ArrowPrimitiveType + ArrowDictionaryKeyType,
    {
        let mut builder: StringDictionaryBuilder<T> =
            self.build_string_dictionary_builder(rows.len());
        for row in rows {
            if let Some(value) = row.get(col_name) {
                if let Some(str_v) = value.as_str() {
                    builder.append(str_v).map(drop)?;
                } else {
                    builder.append_null();
                }
            } else {
                builder.append_null();
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }
}

/// Flattens a list into string values, dropping `Value::Null` in the process.
/// This is useful for interpreting any JSON array as string, dropping nulls.
/// See `json_value_as_string`.
#[inline]
fn flatten_json_string_values(values: &[Value]) -> Vec<Option<String>> {
    values
        .iter()
        .flat_map(|row| {
            if let Value::Array(values) = row {
                values
                    .iter()
                    .map(json_value_as_string)
                    .collect::<Vec<Option<_>>>()
            } else if let Value::Null = row {
                vec![]
            } else {
                vec![json_value_as_string(row)]
            }
        })
        .collect::<Vec<Option<_>>>()
}

#[inline]
fn flatten_json_values(values: &[Value]) -> Vec<Value> {
    values
        .iter()
        .flat_map(|row| {
            if let Value::Array(values) = row {
                values.clone()
            } else if let Value::Null = row {
                vec![Value::Null]
            } else {
                // we interpret a scalar as a single-value list to minimise data loss
                vec![row.clone()]
            }
        })
        .collect()
}

#[inline]
fn json_value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(string) => Some(string.clone()),
        _ => Some(value.to_string()),
    }
}

// TODO: replace it with Arrow v35.0 when it becomes available.
// arrow::compute::kernels::cast_utils::{parse_decimal, Parser}
fn parse_decimal<T: DecimalType>(
    s: &str,
    precision: u8,
    scale: i8,
) -> Result<T::Native, ArrowError> {
    fn is_valid_decimal(s: &str) -> bool {
        let mut seen_dot = false;
        let mut seen_digit = false;
        let mut seen_sign = false;

        for c in s.as_bytes() {
            match c {
                b'-' | b'+' => {
                    if seen_digit || seen_dot || seen_sign {
                        return false;
                    }
                    seen_sign = true;
                }
                b'.' => {
                    if seen_dot {
                        return false;
                    }
                    seen_dot = true;
                }
                b'0'..=b'9' => {
                    seen_digit = true;
                }
                _ => return false,
            }
        }

        seen_digit
    }

    if !is_valid_decimal(s) {
        return Err(ArrowError::ParseError(format!(
            "can't parse the string value {s} to decimal"
        )));
    }
    let mut offset = s.len();
    let len = s.len();
    let mut base = T::Native::usize_as(1);
    #[allow(clippy::cast_sign_loss)]
    let scale_usize = usize::from(scale as u8);

    // handle the value after the '.' and meet the scale
    let delimiter_position = s.find('.');
    match delimiter_position {
        #[allow(clippy::cast_sign_loss)]
        None => {
            base = T::Native::usize_as(10).pow_checked(scale as u32)?;
        }
        #[allow(clippy::cast_possible_truncation)]
        Some(mid) => {
            // there is the '.'
            if len - mid >= scale_usize + 1 {
                // If the string value is "123.12345" and the scale is 2, we should just remain '.12' and drop the '345' value.
                offset -= len - mid - 1 - scale_usize;
            } else {
                // If the string value is "123.12" and the scale is 4, we should append '00' to the tail.
                base = T::Native::usize_as(10).pow_checked((scale_usize + 1 + mid - len) as u32)?;
            }
        }
    };

    // each byte is digit、'-' or '.'
    let bytes = s.as_bytes();
    let mut negative = false;
    let mut result = T::Native::usize_as(0);

    bytes[0..offset]
        .iter()
        .rev()
        .try_for_each::<_, Result<(), ArrowError>>(|&byte| {
            match byte {
                b'-' => {
                    negative = true;
                }
                b'0'..=b'9' => {
                    let add = T::Native::usize_as((byte - b'0') as usize).mul_checked(base)?;
                    result = result.add_checked(add)?;
                    base = base.mul_checked(T::Native::usize_as(10))?;
                }
                // because we have checked the string value
                _ => (),
            }
            Ok(())
        })?;

    if negative {
        result = result.neg_checked()?;
    }

    match T::validate_decimal_precision(result, precision) {
        Ok(_) => Ok(result),
        Err(e) => Err(ArrowError::ParseError(format!(
            "parse decimal overflow: {e}"
        ))),
    }
}
