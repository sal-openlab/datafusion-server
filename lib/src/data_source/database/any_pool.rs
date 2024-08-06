// database/any_pool.rs: Database pools abstraction layer
// Sasaki, Naoki <nsasaki@sal.co.jp> July 27, 2024
//

use futures::stream::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;

use axum::async_trait;
#[cfg(feature = "mysql")]
use sqlx::mysql::MySqlRow;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgRow;
use sqlx::{
    pool::PoolOptions,
    {Pool, Row},
};

pub enum AnyDatabaseRow {
    #[cfg(feature = "postgres")]
    Postgres(PgRow),
    #[cfg(feature = "mysql")]
    MySql(MySqlRow),
}

impl AnyDatabaseRow {
    #[cfg(all(feature = "postgres", feature = "mysql"))]
    pub fn get<'a, T>(&'a self, column: &str) -> Option<T>
    where
        T: for<'r> sqlx::Decode<'r, sqlx::Postgres>
            + sqlx::Type<sqlx::Postgres>
            + for<'r> sqlx::Decode<'r, sqlx::MySql>
            + sqlx::Type<sqlx::MySql>
            + Send
            + 'a,
    {
        match self {
            AnyDatabaseRow::Postgres(row) => row.try_get(column).ok(),
            AnyDatabaseRow::MySql(row) => row.try_get(column).ok(),
        }
    }

    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    pub fn get<'a, T>(&'a self, column: &str) -> Option<T>
    where
        T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send + 'a,
    {
        match self {
            AnyDatabaseRow::Postgres(row) => row.try_get(column).ok(),
        }
    }

    #[cfg(all(feature = "mysql", not(feature = "postgres")))]
    pub fn get<'a, T>(&'a self, column: &str) -> Option<T>
    where
        T: for<'r> sqlx::Decode<'r, sqlx::MySql> + sqlx::Type<sqlx::MySql> + Send + 'a,
    {
        match self {
            AnyDatabaseRow::MySql(row) => row.try_get(column).ok(),
        }
    }

    #[cfg(feature = "postgres")]
    pub fn get_postgres<'a, T: sqlx::Type<sqlx::Postgres> + sqlx::Decode<'a, sqlx::Postgres>>(
        &'a self,
        column: &str,
    ) -> Option<T> {
        match self {
            AnyDatabaseRow::Postgres(row) => row.try_get(column).ok(),
            #[cfg(feature = "mysql")]
            AnyDatabaseRow::MySql(_) => None,
        }
    }

    #[cfg(feature = "mysql")]
    pub fn get_mysql<'a, T: sqlx::Type<sqlx::MySql> + sqlx::Decode<'a, sqlx::MySql>>(
        &'a self,
        column: &str,
    ) -> Option<T> {
        match self {
            AnyDatabaseRow::MySql(row) => row.try_get(column).ok(),
            #[cfg(feature = "postgres")]
            AnyDatabaseRow::Postgres(_) => None,
        }
    }
}

#[async_trait]
pub trait DatabaseOperator {
    async fn fetch_all(&self, query: &str) -> Result<Vec<AnyDatabaseRow>, sqlx::Error>;
    async fn fetch_one(&self, query: &str) -> Result<AnyDatabaseRow, sqlx::Error>;
    fn fetch<'a>(
        &'a self,
        query: &'a str,
    ) -> Pin<Box<dyn Stream<Item = Result<AnyDatabaseRow, sqlx::Error>> + Send + 'a>>;
}

#[cfg(feature = "postgres")]
#[async_trait]
impl DatabaseOperator for Arc<Pool<sqlx::Postgres>> {
    async fn fetch_all(&self, query: &str) -> Result<Vec<AnyDatabaseRow>, sqlx::Error> {
        let rows: Vec<PgRow> = sqlx::query(query).fetch_all(&**self).await?;
        Ok(rows.into_iter().map(AnyDatabaseRow::Postgres).collect())
    }

    async fn fetch_one(&self, query: &str) -> Result<AnyDatabaseRow, sqlx::Error> {
        let row: PgRow = sqlx::query(query).fetch_one(&**self).await?;
        Ok(AnyDatabaseRow::Postgres(row))
    }

    fn fetch<'a>(
        &'a self,
        query: &'a str,
    ) -> Pin<Box<dyn Stream<Item = Result<AnyDatabaseRow, sqlx::Error>> + Send + 'a>> {
        let stream = sqlx::query(query)
            .fetch(&**self)
            .map(|row| row.map(AnyDatabaseRow::Postgres));
        Box::pin(stream)
    }
}

#[cfg(feature = "mysql")]
#[async_trait]
impl DatabaseOperator for Arc<Pool<sqlx::MySql>> {
    async fn fetch_all(&self, query: &str) -> Result<Vec<AnyDatabaseRow>, sqlx::Error> {
        let rows: Vec<MySqlRow> = sqlx::query(query).fetch_all(&**self).await?;
        Ok(rows.into_iter().map(AnyDatabaseRow::MySql).collect())
    }

    async fn fetch_one(&self, query: &str) -> Result<AnyDatabaseRow, sqlx::Error> {
        let row: MySqlRow = sqlx::query(query).fetch_one(&**self).await?;
        Ok(AnyDatabaseRow::MySql(row))
    }

    fn fetch<'a>(
        &'a self,
        query: &'a str,
    ) -> Pin<Box<dyn Stream<Item = Result<AnyDatabaseRow, sqlx::Error>> + Send + 'a>> {
        Box::pin(
            sqlx::query(query)
                .fetch(&**self)
                .map(|row| row.map(AnyDatabaseRow::MySql)),
        )
    }
}

#[derive(Clone)]
pub enum AnyDatabasePool {
    #[cfg(feature = "postgres")]
    Postgres(Arc<Pool<sqlx::Postgres>>),
    #[cfg(feature = "mysql")]
    MySql(Arc<Pool<sqlx::MySql>>),
}

impl AnyDatabasePool {
    pub fn new(url: &str, max_connections: u32) -> Result<Self, sqlx::Error> {
        let scheme = url.split(':').next().unwrap_or("");

        match scheme {
            #[cfg(feature = "postgres")]
            "postgres" => {
                let pool_options: PoolOptions<sqlx::Postgres> = PoolOptions::new()
                    .max_connections(max_connections)
                    .min_connections(1);
                let pool = pool_options.connect_lazy(url)?;
                Ok(AnyDatabasePool::Postgres(Arc::new(pool)))
            }
            #[cfg(feature = "mysql")]
            "mysql" => {
                let pool_options: PoolOptions<sqlx::MySql> = PoolOptions::new()
                    .max_connections(max_connections)
                    .min_connections(1);
                let pool = pool_options.connect_lazy(url)?;
                Ok(AnyDatabasePool::MySql(Arc::new(pool)))
            }
            _ => Err(sqlx::Error::Configuration(
                format!("Unsupported database scheme: {scheme}").into(),
            )),
        }
    }
}

#[async_trait]
impl DatabaseOperator for AnyDatabasePool {
    async fn fetch_all(&self, query: &str) -> Result<Vec<AnyDatabaseRow>, sqlx::Error> {
        match self {
            #[cfg(feature = "postgres")]
            AnyDatabasePool::Postgres(pool) => pool.fetch_all(query).await,
            #[cfg(feature = "mysql")]
            AnyDatabasePool::MySql(pool) => pool.fetch_all(query).await,
        }
    }

    async fn fetch_one(&self, query: &str) -> Result<AnyDatabaseRow, sqlx::Error> {
        match self {
            #[cfg(feature = "postgres")]
            AnyDatabasePool::Postgres(pool) => pool.fetch_one(query).await,
            #[cfg(feature = "mysql")]
            AnyDatabasePool::MySql(pool) => pool.fetch_one(query).await,
        }
    }

    fn fetch<'a>(
        &'a self,
        query: &'a str,
    ) -> Pin<Box<dyn Stream<Item = Result<AnyDatabaseRow, sqlx::Error>> + Send + 'a>> {
        match self {
            #[cfg(feature = "postgres")]
            AnyDatabasePool::Postgres(pool) => pool.fetch(query),
            #[cfg(feature = "mysql")]
            AnyDatabasePool::MySql(pool) => pool.fetch(query),
        }
    }
}
