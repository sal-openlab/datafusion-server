// object_store/writer.rs

use datafusion::{
    common::config::{CsvOptions, JsonOptions, ParquetOptions, TableParquetOptions},
    dataframe::DataFrameWriteOptions,
    execution::context::SessionContext,
};

use crate::request::body::{DataSource, DataSourceFormat, DataSourceOption};
use crate::response::http_error::ResponseError;

pub async fn write(ctx: &SessionContext, data_source: &DataSource) -> Result<(), ResponseError> {
    log::debug!("object_store::writer(): {:?}", data_source);

    let options = match &data_source.options {
        Some(options) => options.clone(),
        None => DataSourceOption::new(),
    };

    let write_options = DataFrameWriteOptions::new()
        // TODO: raise error when options.overwrite is true and already exists target file
        // .with_overwrite(options.overwrite.unwrap_or(false))
        .with_single_file_output(true);

    let df = ctx
        .sql(&format!("SELECT * FROM {}", data_source.name))
        .await?;

    match data_source.format {
        DataSourceFormat::Csv => {
            let csv_options = CsvOptions::default()
                .with_has_header(options.has_header.unwrap_or(true))
                .with_delimiter(options.delimiter.unwrap_or(',') as u8);

            df.write_csv(&data_source.location, write_options, Some(csv_options))
                .await?;
        }
        DataSourceFormat::NdJson => {
            let ndjson_options = JsonOptions::default();
            df.write_json(&data_source.location, write_options, Some(ndjson_options))
                .await?;
        }
        DataSourceFormat::Parquet => {
            let parquet_options = ParquetOptions {
                compression: Some("snappy".to_string()),
                created_by: format!("datafusion-server v{}", env!("CARGO_PKG_VERSION")),
                ..Default::default()
            };

            df.write_parquet(
                &data_source.location,
                write_options,
                Some(TableParquetOptions {
                    global: parquet_options,
                    ..Default::default()
                }),
            )
            .await?;
        }
        _ => {
            return Err(ResponseError::unsupported_type(format!(
                "write to object store is not supported {:?}",
                &data_source.format
            )));
        }
    }

    Ok(())
}
