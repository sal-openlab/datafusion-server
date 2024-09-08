// database/any_pool.rs: Database pools abstraction layer
// Sasaki, Naoki <nsasaki@sal.co.jp> July 27, 2024
//

use futures::stream::{Stream, StreamExt};
use std::future::Future;
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

#[derive(Debug)]
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
    #[allow(dead_code)]
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
    async fn execute(&self, sql: &str) -> Result<(u64, Option<u64>), sqlx::Error>;
    async fn begin_transaction(
        &self,
    ) -> Result<Box<dyn TransactionHandler + Send + 'static>, sqlx::Error>;
}

#[async_trait]
pub trait TransactionHandler: Send {
    async fn commit(self: Box<Self>) -> Result<(), sqlx::Error>;
    async fn rollback(self: Box<Self>) -> Result<(), sqlx::Error>;
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

    async fn execute(&self, sql: &str) -> Result<(u64, Option<u64>), sqlx::Error> {
        let result = sqlx::query(sql).execute(&**self).await?;
        Ok((result.rows_affected(), None))
    }

    async fn begin_transaction(
        &self,
    ) -> Result<Box<dyn TransactionHandler + Send + 'static>, sqlx::Error> {
        let tx = self.begin().await?;
        Ok(Box::new(PostgresTransactionHandler { tx }))
    }
}

pub struct PostgresTransactionHandler {
    tx: sqlx::Transaction<'static, sqlx::Postgres>,
}

#[async_trait]
impl TransactionHandler for PostgresTransactionHandler {
    async fn commit(self: Box<Self>) -> Result<(), sqlx::Error> {
        self.tx.commit().await
    }

    async fn rollback(self: Box<Self>) -> Result<(), sqlx::Error> {
        self.tx.rollback().await
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

    async fn execute(&self, sql: &str) -> Result<(u64, Option<u64>), sqlx::Error> {
        let result = sqlx::query(sql).execute(&**self).await?;
        Ok((result.rows_affected(), Some(result.last_insert_id())))
    }

    async fn begin_transaction(
        &self,
    ) -> Result<Box<dyn TransactionHandler + Send + 'static>, sqlx::Error> {
        let tx = self.begin().await?;
        Ok(Box::new(MySqlTransactionHandler { tx }))
    }
}

pub struct MySqlTransactionHandler {
    tx: sqlx::Transaction<'static, sqlx::MySql>,
}

#[async_trait]
impl TransactionHandler for MySqlTransactionHandler {
    async fn commit(self: Box<Self>) -> Result<(), sqlx::Error> {
        self.tx.commit().await
    }

    async fn rollback(self: Box<Self>) -> Result<(), sqlx::Error> {
        self.tx.rollback().await
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

    async fn execute(&self, sql: &str) -> Result<(u64, Option<u64>), sqlx::Error> {
        match self {
            #[cfg(feature = "postgres")]
            AnyDatabasePool::Postgres(pool) => pool.execute(sql).await,
            #[cfg(feature = "mysql")]
            AnyDatabasePool::MySql(pool) => pool.execute(sql).await,
        }
    }

    async fn begin_transaction(
        &self,
    ) -> Result<Box<dyn TransactionHandler + Send + 'static>, sqlx::Error> {
        match self {
            #[cfg(feature = "postgres")]
            AnyDatabasePool::Postgres(pool) => pool.begin_transaction().await,
            #[cfg(feature = "mysql")]
            AnyDatabasePool::MySql(pool) => pool.begin_transaction().await,
        }
    }
}

#[async_trait]
pub trait DatabaseOperatorExt: DatabaseOperator {
    async fn execute_transaction<F, Fut, T>(
        &self,
        operation: F,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce(&dyn TransactionHandler) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send,
        T: Send + 'static,
    {
        let tx = self.begin_transaction().await?;

        match operation(tx.as_ref()).await {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(err) => {
                tx.rollback().await?;
                Err(err)
            }
        }
    }

    // MEMO: Currently unused
    // async fn execute_transaction_sync<F, T>(
    //     &self,
    //     operation: F,
    // ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    // where
    //     F: FnOnce(&dyn TransactionHandler) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    //         + Send
    //         + 'static,
    //     T: Send + 'static,
    // {
    //     let tx = self.begin_transaction().await?;
    //
    //     match operation(tx.as_ref()) {
    //         Ok(result) => {
    //             tx.commit().await?;
    //             Ok(result)
    //         }
    //         Err(err) => {
    //             tx.rollback().await?;
    //             Err(err)
    //         }
    //     }
    // }
}

impl<T: DatabaseOperator + ?Sized> DatabaseOperatorExt for T {}
