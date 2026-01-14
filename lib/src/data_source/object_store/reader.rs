// object_store/reader.rs

#[cfg(feature = "avro")]
use datafusion::datasource::file_format::options::AvroReadOptions;
use datafusion::{
    datasource::file_format::options::{CsvReadOptions, NdJsonReadOptions, ParquetReadOptions},
    execution::context::SessionContext,
};

use crate::data_source::location;
use crate::request::body::{DataSource, DataSourceFormat, DataSourceOption};
use crate::response::http_error::ResponseError;

pub async fn register(ctx: &SessionContext, data_source: &DataSource) -> Result<(), ResponseError> {
    log::debug!("object_store::register(): {data_source:?}");

    let uri = location::uri::to_parts(&data_source.location)?;

    let location = if location::uri::scheme(&uri).unwrap_or(location::uri::SupportedScheme::File)
        == location::uri::SupportedScheme::File
    {
        location::file::create_data_file_path(&data_source.location)?
    } else {
        data_source.location.clone()
    };

    match data_source.format {
        DataSourceFormat::Csv => register_csv(ctx, data_source, &location).await?,
        DataSourceFormat::NdJson => register_ndjson(ctx, data_source, &location).await?,
        DataSourceFormat::Parquet => register_parquet(ctx, data_source, &location).await?,
        #[cfg(feature = "avro")]
        DataSourceFormat::Avro => register_avro(ctx, data_source, &location).await?,
        _ => {
            return Err(ResponseError::unsupported_type(format!(
                "read from object store is not supported {:?}",
                &data_source.format
            )));
        }
    }

    Ok(())
}

async fn register_csv(
    ctx: &SessionContext,
    data_source: &DataSource,
    location: &str,
) -> Result<(), ResponseError> {
    log::debug!("object_store::register_csv(): {location}");

    let options = match &data_source.options {
        Some(options) => options.clone(),
        None => DataSourceOption::new(),
    };

    let csv_options = CsvReadOptions::default()
        .has_header(options.has_header.unwrap_or(true))
        .delimiter(options.delimiter.unwrap_or(',') as u8);

    let arrow_schema;
    let csv_options = if let Some(schema) = &data_source.schema {
        arrow_schema = schema.to_arrow_schema()?;
        csv_options.schema(&arrow_schema)
    } else {
        csv_options.schema_infer_max_records(options.infer_schema_rows.unwrap_or(100))
    };

    ctx.register_csv(&data_source.name, location, csv_options)
        .await?;

    Ok(())
}

async fn register_ndjson(
    ctx: &SessionContext,
    data_source: &DataSource,
    location: &str,
) -> Result<(), ResponseError> {
    log::debug!("object_store::register_ndjson(): {location}");

    let options = match &data_source.options {
        Some(options) => options.clone(),
        None => DataSourceOption::new(),
    };

    let mut ndjson_options = NdJsonReadOptions::default();

    let arrow_schema;
    let ndjson_options = if let Some(schema) = &data_source.schema {
        arrow_schema = schema.to_arrow_schema()?;
        ndjson_options.schema(&arrow_schema)
    } else {
        ndjson_options.schema_infer_max_records = options.infer_schema_rows.unwrap_or(100);
        ndjson_options
    };

    ctx.register_json(&data_source.name, location, ndjson_options)
        .await?;

    Ok(())
}

async fn register_parquet(
    ctx: &SessionContext,
    data_source: &DataSource,
    location: &str,
) -> Result<(), ResponseError> {
    log::debug!("object_store::register_parquet(): {location}");
    ctx.register_parquet(&data_source.name, location, ParquetReadOptions::default())
        .await?;
    Ok(())
}

#[cfg(feature = "avro")]
async fn register_avro(
    ctx: &SessionContext,
    data_source: &DataSource,
    location: &str,
) -> Result<(), ResponseError> {
    log::debug!("object_store::register_avro(): {location}");

    let mut avro_options = AvroReadOptions::default();

    let arrow_schema;
    if data_source.schema.is_some() {
        arrow_schema = data_source.schema.as_ref().unwrap().to_arrow_schema()?;
        avro_options = avro_options.schema(&arrow_schema);
    }

    ctx.register_avro(&data_source.name, location, avro_options)
        .await?;

    Ok(())
}
