// exec_processor.rs - Executes defined processors
// Sasaki, Naoki <nsasaki@sal.co.jp> April 7, 2024
//

use crate::plugin::plugin_manager::PluginManager;
use crate::request::body::{PluginOption, PostProcessor};
use crate::response::http_error::ResponseError;
use datafusion::arrow::record_batch::RecordBatch;

pub fn post_processors(
    post_processors: Vec<PostProcessor>,
    mut record_batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, ResponseError> {
    for processor in post_processors {
        if let Some(module) = processor.module {
            let plugin_options = match &processor.plugin_options {
                Some(options) => options.clone(),
                None => PluginOption::new(),
            };

            record_batches = PluginManager::global().py_processor_exec(
                &record_batches,
                &module,
                &plugin_options.options,
            )?;
        } else {
            return Err(ResponseError::request_validation(
                "Must be defined processor module",
            ));
        }
    }
    Ok(record_batches)
}
