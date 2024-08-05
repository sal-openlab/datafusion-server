// database/table_resolver.ra: Cacheable table resolver
// Sasaki, Naoki <nsasaki@sal.co.jp> July 27, 2024
//

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use datafusion::{datasource::TableProvider, error::DataFusionError};

use crate::data_source::database::{
    any_pool::AnyDatabasePool, engine_type::DatabaseEngineType, table_provider::DatabaseTable,
};

#[derive(Clone)]
pub struct TableResolver {
    engine_type: DatabaseEngineType,
    pool: AnyDatabasePool,
    database: String,
    tables: Arc<RwLock<HashMap<String, Arc<dyn TableProvider>>>>,
    schema_cache: bool,
}

impl TableResolver {
    pub fn new(
        engine_type: DatabaseEngineType,
        pool: AnyDatabasePool,
        database: &str,
        schema_cache: bool,
    ) -> Self {
        TableResolver {
            engine_type,
            pool,
            database: database.to_string(),
            tables: Arc::new(RwLock::new(HashMap::new())),
            schema_cache,
        }
    }

    pub async fn get_table(
        &self,
        table_name: &str,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        if self.schema_cache {
            let tables = self.tables.read().unwrap();
            if let Some(table) = tables.get(table_name) {
                return Ok(table.clone());
            }
        }

        let table = Arc::new(
            DatabaseTable::new(
                &self.engine_type,
                self.pool.clone(),
                &self.database,
                table_name,
            )
            .await?,
        );

        if self.schema_cache {
            let mut tables = self.tables.write().unwrap();
            tables.insert(table_name.to_string(), table.clone());
        }

        Ok(table)
    }
}
