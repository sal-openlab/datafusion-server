// database/table_register.rs
// Sasaki, Naoki <nsasaki@sal.co.jp> July 27, 2024
//

use futures::future::BoxFuture;

use datafusion::{
    error::DataFusionError,
    execution::context::SessionContext,
    sql::sqlparser::{
        ast::{Query, Select, SetExpr, Statement, TableFactor},
        dialect::GenericDialect,
        parser::Parser,
    },
};

use crate::settings::Settings;

pub async fn from_sql(ctx: &SessionContext, sql: &str) -> Result<(), DataFusionError> {
    let ast =
        Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| DataFusionError::SQL(e, None))?;

    for statement in ast {
        if let Statement::Query(query) = statement {
            process_query(ctx, &query).await?;
        }
    }

    Ok(())
}

fn process_query<'a>(
    ctx: &'a SessionContext,
    query: &'a Query,
) -> BoxFuture<'a, Result<(), DataFusionError>> {
    Box::pin(async move {
        match &*query.body {
            SetExpr::Select(select) => process_select(ctx, select).await,
            SetExpr::Query(subquery) => process_query(ctx, subquery).await,
            _ => Ok(()), // ignores `Insert`, `Update` and `Delete` statement
        }
    })
}

async fn process_select(ctx: &SessionContext, select: &Select) -> Result<(), DataFusionError> {
    for table_with_joins in &select.from {
        if let TableFactor::Table { name, .. } = &table_with_joins.relation {
            let table_identifier = &name.0[0].value;
            let (table_name, namespace) = if let Some(pos) = table_identifier.rfind('@') {
                let (left, right) = table_identifier.split_at(pos);
                (left.to_string(), Some(right[1..].to_string()))
            } else {
                (table_identifier.to_string(), None)
            };

            if let Some(namespace) = namespace {
                if let Some(resolver) = Settings::global()
                    .database_pool_manager
                    .resolvers
                    .get(&namespace)
                {
                    let full_table_name = format!("{table_name}@{namespace}");

                    if ctx.table_exist(&full_table_name)? {
                        ctx.deregister_table(&full_table_name)?;
                    }

                    ctx.register_table(&full_table_name, resolver.get_table(&table_name).await?)?;
                }
            }
        }
    }

    Ok(())
}
