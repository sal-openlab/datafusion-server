// database/table_processor.rs
// Sasaki, Naoki <nsasaki@sal.co.jp> August 31, 2024
//

use std::sync::Arc;

use datafusion::{
    error::DataFusionError,
    sql::sqlparser::{
        ast::{
            AssignmentTarget, Expr, Insert, ObjectName, Query, Select, SetExpr, Statement,
            TableFactor,
        },
        dialect::GenericDialect,
        parser::Parser,
    },
};
use futures::{future::BoxFuture, stream::StreamExt};

use crate::context::session::{ConcurrentSessionContext, Session};
use crate::data_source::database::{
    any_pool::DatabaseOperatorExt, column_binder, table_resolver::TableResolver,
};
use crate::settings::Settings;

pub async fn from_sql(
    ctx: &ConcurrentSessionContext,
    sql: &str,
) -> Result<String, DataFusionError> {
    let ast =
        Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| DataFusionError::SQL(e, None))?;

    let mut query_statements = vec![];

    for mut statement in ast {
        match &mut statement {
            Statement::Query(query) => {
                process_query(ctx, query).await?;
                query_statements.push(query.to_string());
            }
            Statement::Insert(_) => {
                process_insert(ctx, &mut statement).await?;
            }
            Statement::Update { .. } => {
                process_update(ctx, &statement).await?;
            }
            _ => {}
        }
    }

    Ok(query_statements.join(";\n"))
}

fn process_query<'a>(
    ctx: &'a ConcurrentSessionContext,
    query: &'a Query,
) -> BoxFuture<'a, Result<(), DataFusionError>> {
    Box::pin(async move {
        match &*query.body {
            SetExpr::Select(select) => process_select(ctx, select).await,
            SetExpr::Query(subquery) => process_query(ctx, subquery).await,
            _ => Ok(()),
        }
    })
}

async fn process_select(
    ctx: &ConcurrentSessionContext,
    select: &Select,
) -> Result<(), DataFusionError> {
    for table_with_joins in &select.from {
        if let TableFactor::Table { name, .. } = &table_with_joins.relation {
            let (table_name, namespace) = table_with_namespace(name);

            if let Some(resolver) = get_resolver(&namespace) {
                let full_table_name = format!("{table_name}@{}", namespace.clone().unwrap());
                let df_ctx = &ctx.write().await.df_ctx;

                if df_ctx.table_exist(&full_table_name)? {
                    df_ctx.deregister_table(&full_table_name)?;
                }

                df_ctx.register_table(&full_table_name, resolver.get_table(&table_name).await?)?;
            }
        }
    }

    Ok(())
}

async fn process_insert(
    ctx: &ConcurrentSessionContext,
    statement: &mut Statement,
) -> Result<(), DataFusionError> {
    if let Statement::Insert(insert) = statement {
        let (table_name, namespace) = table_with_namespace(&insert.table_name);

        if let Some(resolver) = get_resolver(&namespace).map(Arc::clone) {
            if let Some(source) = &insert.source {
                match &*source.body {
                    SetExpr::Values(_) => {
                        remove_table_namespace(&mut insert.table_name);
                        log::debug!("insert with values: {:?}", &statement.to_string());
                        resolver.sql_exec(&statement.to_string()).await?;
                    }
                    SetExpr::Select(select) => {
                        let mut query = select.to_string();
                        if let Some(limit) = &source.limit {
                            query.push_str(&format!(" LIMIT {limit}"));
                        }
                        if let Some(offset) = &source.offset {
                            query.push_str(&format!(" OFFSET {}", offset.value));
                        }

                        insert_to_database(ctx, insert, resolver, &table_name, &query).await?;
                    }
                    SetExpr::Query(query) => {
                        insert_to_database(ctx, insert, resolver, &table_name, &query.to_string())
                            .await?;
                    }
                    _ => {
                        return Err(DataFusionError::Execution(
                            "`INSERT` with no sources".into(),
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}

async fn insert_to_database(
    ctx: &ConcurrentSessionContext,
    insert: &Insert,
    resolver: Arc<TableResolver>,
    table_name: &str,
    query_source: &str,
) -> Result<(), DataFusionError> {
    log::debug!("query source: {query_source:?}");

    let dataframe = ctx.execute_logical_plan(query_source).await?;
    let mut stream = dataframe.execute_stream().await?;

    let pre_sql = format!(
        r#"INSERT INTO "{table_name}" ({}) VALUES"#,
        insert
            .columns
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<String>>()
            .join(",")
    );

    resolver
        .clone()
        .pool
        .execute_transaction({
            let pre_sql = pre_sql.clone();
            move |_| async move {
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    let mut placeholders = Vec::new();
                    let mut bind_index = 1_usize;

                    for _ in 0..batch.num_rows() {
                        let mut row_placeholders = Vec::new();
                        for _ in 0..batch.num_columns() {
                            row_placeholders.push(format!("${bind_index}"));
                            bind_index += 1;
                        }
                        placeholders.push(format!("({})", row_placeholders.join(",")));
                    }

                    let sql = format!("{pre_sql} {}", placeholders.join(","));
                    let mut query = column_binder::create_query(&resolver.engine_type, &sql);

                    for row in 0..batch.num_rows() {
                        for col in 0..batch.num_columns() {
                            let column_array = batch.column(col);
                            query = column_binder::bind_query(query, column_array, row);
                        }
                    }

                    log::debug!("insert with generated values: {:?}", sql);

                    let (effected, _) = column_binder::execute_query(query, &resolver.pool).await?;

                    log::debug!("effected rows: {effected}");
                }
                Ok(())
            }
        })
        .await?;

    Ok(())
}

async fn process_update(
    ctx: &ConcurrentSessionContext,
    statement: &Statement,
) -> Result<(), DataFusionError> {
    if let Statement::Update {
        table,
        assignments,
        from,
        selection,
        .. // returning
    } = statement
    {
        if let TableFactor::Table { name, .. } = &table.relation {
            let (table_name, namespace) = table_with_namespace(name);

            if let Some(resolver) = get_resolver(&namespace) {
                if from.is_some() {
                    return Err(DataFusionError::Execution(
                        "Currently unsupported `UPDATE` with `FROM` sources".into(),
                    ));
                }

                let mut column_values = vec![];

                for assignment in assignments {
                    if let AssignmentTarget::ColumnName(column) = &assignment.target {
                        column_values.push((column.0[0].to_string(), assignment.value.clone()));
                    } else {
                        return Err(DataFusionError::Execution(
                            "Not found assigment column".into(),
                        ));
                    }
                }

                let mut bind_ops = vec![];
                let mut bind_index = 1_usize;

                for (column, value) in &column_values {
                    match value {
                        Expr::Value(v) => bind_ops.push(format!("{column}={v}")),
                        Expr::Subquery(_) => {
                            bind_ops.push(format!("{column}=${bind_index}"));
                            bind_index += 1;
                        }
                        _ => {
                            return Err(DataFusionError::Execution(
                                "Unsupported assignment value".into(),
                            ))
                        }
                    }
                }

                let mut sql = format!("UPDATE {table_name} SET {}", bind_ops.join(","));

                if let Some(selection) = &selection {
                    sql.push_str(&format!(" WHERE {selection}"));
                }

                let mut bind_query = column_binder::create_query(&resolver.engine_type, &sql);
                for (_, value) in &column_values {
                    if let Expr::Subquery(query_source) = value {
                        let dataframe =
                            ctx.execute_logical_plan(&query_source.to_string()).await?;
                        let mut stream = dataframe.execute_stream().await?;

                        if let Some(batch) = stream.next().await {
                            let batch = batch?;

                            if batch.num_rows() != 1 {
                                return Err(DataFusionError::Execution(
                                    "Not assignment multiple values".to_string(),
                                ));
                            } else if batch.num_columns() == 0 {
                                return Err(DataFusionError::Execution(
                                    "No assignment value available (0 column)".to_string(),
                                ));
                            }

                            let column_array = batch.column(0);
                            bind_query = column_binder::bind_query(bind_query, column_array, 0);
                        } else {
                            return Err(DataFusionError::Execution(
                                "No assignment value available (0 row)".to_string(),
                            ));
                        }
                    }
                }

                let (effected, _) =
                    column_binder::execute_query(bind_query, &resolver.pool)
                        .await
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                log::debug!("effected rows: {effected}");
            }
        }
    }

    Ok(())
}

fn get_resolver(namespace: &Option<String>) -> Option<&Arc<TableResolver>> {
    if let Some(namespace) = namespace {
        Settings::global()
            .database_pool_manager
            .resolvers
            .get(namespace)
    } else {
        None
    }
}

fn table_with_namespace(name: &ObjectName) -> (String, Option<String>) {
    fn quote_identifier(value: &str, quote_style: Option<char>) -> String {
        if let Some(quote) = quote_style {
            format!("{quote}{value}{quote}")
        } else {
            value.to_owned()
        }
    }

    let table_ident = &name.0[0];
    let quoted_identifier = quote_identifier(&table_ident.value, table_ident.quote_style);

    if let Some(at_pos) = table_ident.value.rfind('@') {
        let (base_name, namespace) = table_ident.value.split_at(at_pos);
        let quoted_base_name = quote_identifier(base_name, table_ident.quote_style);
        (quoted_base_name, Some(namespace[1..].to_owned()))
    } else {
        (quoted_identifier, None)
    }
}

// fn table_from_statement(statement: &mut Statement) -> Option<&mut ObjectName> {
//     match statement {
//         Statement::Insert(insert_stmt) => Some(&mut insert_stmt.table_name),
//         Statement::Update { table, .. } => match &mut table.relation {
//             TableFactor::Table { name, .. } => Some(name),
//             _ => None,
//         },
//         Statement::Delete(delete_stmt) => delete_stmt.tables.first_mut(),
//         _ => None,
//     }
// }

fn remove_table_namespace(table_name: &mut ObjectName) {
    for ident in &mut table_name.0 {
        if let Some(at_pos) = ident.value.find('@') {
            ident.value = ident.value[..at_pos].to_string();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::data_source::database::table_processor::table_with_namespace;
    use datafusion::sql::sqlparser::{
        ast::{Ident, ObjectName, SetExpr, Statement},
        dialect::GenericDialect,
        parser::Parser,
    };

    // #[test]
    // fn insert_from_select() {
    //     let sql = r#"
    //         INSERT INTO table1 (column1, column2)
    //         SELECT column1, column2 FROM store WHERE "State"='CA';
    //     "#;
    //     // let sql = "
    //     //     INSERT INTO table1 (column1, column2)
    //     //     VALUES('v1', 'v2');
    //     // ";
    //
    //     let ast = Parser::parse_sql(&GenericDialect {}, sql).expect("Unable to parse SQL");
    //
    //     for statement in ast {
    //         if let Statement::Insert(insert) = statement {
    //             println!("STMT: {insert:?}");
    //             if let Some(query) = insert.source {
    //                 match *query.body {
    //                     SetExpr::Values(values) => {
    //                         println!("Using values: {values:?}");
    //                     }
    //                     SetExpr::Select(query) => {
    //                         println!("Using SELECT: {:?}", query.to_string());
    //                     }
    //                     _ => {
    //                         println!("Other source");
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //
    //     assert_eq!(true, true);
    // }

    #[test]
    fn get_table_with_namespace() {
        let table_name = ObjectName(vec![Ident::new("table_name@namespace")]);
        let (table, namespace) = table_with_namespace(&table_name);
        assert_eq!(table, "table_name");
        assert_eq!(namespace.unwrap(), "namespace");

        let table_name = ObjectName(vec![Ident::with_quote('"', "table_name@namespace")]);
        let (table, namespace) = table_with_namespace(&table_name);
        assert_eq!(table, r#""table_name""#);
        assert_eq!(namespace.unwrap(), "namespace");
    }

    #[test]
    fn get_table_without_namespace() {
        let table_name = ObjectName(vec![Ident::new("table_name")]);
        let (table, namespace) = table_with_namespace(&table_name);
        assert_eq!(table, "table_name");
        assert_eq!(namespace, None);

        let table_name = ObjectName(vec![Ident::with_quote('"', "table_name")]);
        let (table, namespace) = table_with_namespace(&table_name);
        assert_eq!(table, r#""table_name""#);
        assert_eq!(namespace, None);
    }

    // #[test]
    // fn update_from_select() {
    //     let sql = r#"
    //         UPDATE "table_name" SET column1 = 'new_value' WHERE column_id = 1 AND (column_foo = 'bar' OR price < 1000);
    //     "#;
    //
    //     let ast = Parser::parse_sql(&GenericDialect {}, sql).expect("Unable to parse SQL");
    //
    //     #[allow(clippy::needless_for_each)]
    //     for statement in &ast {
    //         if let Statement::Update {
    //             table,
    //             assignments,
    //             from,
    //             selection,
    //             returning,
    //         } = statement
    //         {
    //             //println!("statement: {statement:?}");
    //             println!("table: {table:?}");
    //             println!("table: {table}");
    //             println!();
    //             println!("assignments: {assignments:?}");
    //             assignments.iter().for_each(|a| println!("assignment: {a}"));
    //             println!();
    //             println!("from: {from:?}");
    //             println!();
    //             println!("selection: {selection:?}");
    //             if let Some(s) = selection {
    //                 println!("selection: {s}");
    //             }
    //             println!();
    //             println!("returning: {returning:?}");
    //         }
    //     }
    //
    //     assert_eq!(true, true);
    // }
}
