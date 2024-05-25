// data_source/location/file.rs

use crate::data_source::location::uri;
use crate::response::http_error::ResponseError;
use crate::settings::Settings;

pub fn create_data_file_path(file: &str) -> Result<String, ResponseError> {
    let mut file_path = std::path::PathBuf::from(&Settings::global().server.data_dir);
    file_path.push(uri::to_file_path_and_name(file)?);
    match file_path.to_str() {
        Some(v) => Ok(v.to_owned()),
        None => Err(ResponseError::unsupported_format(format!(
            "Can not decode file path string {file:?}"
        ))),
    }
}
