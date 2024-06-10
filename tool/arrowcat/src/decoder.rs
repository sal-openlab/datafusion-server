// decoder.rs

use std::io;

use base64::Engine;

pub fn base64(encoded: &str) -> Result<Vec<u8>, io::Error> {
    let decoded_data = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    Ok(decoded_data)
}
