// database/engine_type.rs: External database engine types
// Sasaki, Naoki <nsasaki@sal.co.jp> July 27, 2024
//

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub enum DatabaseEngineType {
    #[cfg(feature = "postgres")]
    Postgres,
    #[cfg(feature = "mysql")]
    MySQL,
}

impl DatabaseEngineType {
    pub fn from_scheme(scheme: &str) -> Result<Self, sqlx::error::Error> {
        match scheme {
            #[cfg(feature = "postgres")]
            "postgres" => Ok(Self::Postgres),
            #[cfg(feature = "mysql")]
            "mysql" => Ok(Self::MySQL),
            _ => Err(sqlx::error::Error::Protocol(format!(
                "Unsupported external database engine: {scheme}"
            ))),
        }
    }
}
