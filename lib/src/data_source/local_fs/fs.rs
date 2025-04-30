// write/fs.rs - File system utils
// Sasaki, Naoki <nsasaki@sal.co.jp> May 5, 2023
//

use crate::response::http_error::ResponseError;
use std::path::Path;

pub fn mkdir_if_not_exists(path: &Path, all: bool) -> Result<(), ResponseError> {
    if !path.to_path_buf().exists() {
        log::debug!("creating directory {path:?}");

        if all {
            std::fs::create_dir_all(path)?;
        } else {
            std::fs::create_dir(path)?;
        }
    }

    Ok(())
}
