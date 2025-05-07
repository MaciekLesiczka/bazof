use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::Arc;

use bazof::AsOf::{Current, EventTime};
use bazof::{AsOf};
use bazof_datafusion::BazofTableProvider;
use chrono::{DateTime, TimeZone, Utc};
use datafusion::logical_expr::sqlparser::ast::Visitor;
use datafusion::logical_expr::sqlparser::ast::{
    Expr, Ident, ObjectName, TableFactor, TableVersion, Value, VisitMut, VisitorMut,
};
use datafusion::prelude::*;
use datafusion::sql::parser::Statement;
use object_store::local::LocalFileSystem;
use object_store::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut workspace_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    workspace_dir.pop();
    workspace_dir.pop();

    let test_data_path = workspace_dir.join("test-data");

    let absolute_path = test_data_path.canonicalize()?;

    let path_str = absolute_path.to_string_lossy().to_string();

    let store_path = Path::from(path_str);

    let local_store = Arc::new(LocalFileSystem::new());

    let ctx = SessionContext::new();
    //
    // println!("Querying Last 12 months sales for Apple and Google");
    // let provider = BazofTableProvider::current(
    //     store_path.clone(),
    //     local_store.clone(),
    //     "ltm_revenue".to_string(),
    // )
    // .await?;
    //
    // ctx.register_table("ltm_revenue", Arc::new(provider))?;
    //
    //
    //let df = ctx.sql("SELECT key as symbol, value as revenue FROM ltm_revenue FOR SYSTEM_TIME AS OF '2021-09-01 T10:00:00.7230011' WHERE key IN ('AAPL', 'GOOG') ORDER BY key").await?;
    // df.show().await?;
    //
    // println!("\nQuerying Last 12 months sales for Apple and Google as of 2019-01-17");
    // let event_time = Utc.with_ymd_and_hms(2019, 1, 17, 0, 0, 0).unwrap();
    //
    // let provider = BazofTableProvider::as_of(
    //     store_path.clone(),
    //     local_store.clone(),
    //     "ltm_revenue".to_string(),
    //     event_time,
    // )
    // .await?;
    //
    // // Register the table with DataFusion
    // ctx.register_table("ltm_revenue_jan17", Arc::new(provider))?;
    //
    // let df = ctx.sql("SELECT key as symbol, value as revenue FROM ltm_revenue_jan17 WHERE key IN ('AAPL', 'GOOG') ORDER BY key").await?;
    // df.show().await?;
    //
    //

    //----
    let mut stmt = ctx.state().sql_to_statement("SELECT key as symbol, value as revenue FROM ltm_revenue FOR SYSTEM_TIME AS OF '2019-01-17T00:00:00.000Z' LIMIT 10", "snowflake")?;

    let tables = visit_statement(&mut stmt);

    for (original_name, name, as_of) in tables? {
        let table_ref = name.to_string();
        let provider = BazofTableProvider::new(
            store_path.clone(),
            local_store.clone(),
            original_name.to_string(),
            as_of,
        )
        .await?;

        ctx.register_table(table_ref, Arc::new(provider))?;
    }

    println!("MODIFIED --->>>> {}", stmt);

    let plan = ctx.state().statement_to_plan(stmt).await?;

    let df = ctx.execute_logical_plan(plan).await?;

    df.show().await?;

    Ok(())
}

fn visit_statement(
    statement: &mut Statement,
) -> Result<Vec<(ObjectName, ObjectName, AsOf)>, Box<dyn std::error::Error>> {
    struct RewriteVersionIntoTableIdent {
        relations: Vec<(ObjectName, ObjectName, AsOf)>,
    }
    impl VisitorMut for RewriteVersionIntoTableIdent {
        type Break = Box<dyn std::error::Error>;
        fn post_visit_table_factor(
            &mut self,
            table_factor: &mut TableFactor,
        ) -> ControlFlow<Self::Break> {
            match rewrite_and_extract_versioned_tables(table_factor) {
                Ok(Some(table)) => {
                    self.relations.push(table);
                    ControlFlow::Continue(())
                }
                Err(e) => ControlFlow::Break(e),
                _ => ControlFlow::Continue(()),
            }
        }
    }

    fn rewrite_and_extract_versioned_tables(
        table_factor: &mut TableFactor,
    ) -> Result<Option<(ObjectName, ObjectName, AsOf)>, Box<dyn std::error::Error>> {
        if let TableFactor::Table { name, version, .. } = table_factor {
            let original_name = name.clone();
            let as_of: Result<AsOf, Box<dyn std::error::Error>> = {
                if let Some(TableVersion::ForSystemTimeAsOf(Expr::Value(
                    Value::SingleQuotedString(str),
                ))) = version
                {
                    let event_time =
                        DateTime::parse_from_rfc3339(&str).map(|dt| dt.with_timezone(&Utc))?;
                    if let ObjectName(idents) = name {
                        let mut new_idents: Vec<Ident> = Vec::with_capacity(idents.len());

                        for i in 0..idents.len() - 1 {
                            new_idents.push(idents[i].clone());
                        }

                        if let Some(last) = idents.last() {
                            new_idents.push(Ident {
                                value: format!("{}__{}", last.value, event_time.timestamp_millis()),
                                quote_style: last.quote_style,
                                span: last.span.clone(),
                            });
                        }

                        *name = ObjectName(new_idents);
                        *version = None;
                    }
                    Ok(EventTime(event_time))
                } else {
                    Ok(Current)
                }
            };

            return Ok(Some((original_name, name.clone(), as_of?)));
        }
        Ok(None)
    }

    let mut visitor = RewriteVersionIntoTableIdent { relations: vec![] };

    match statement {
        Statement::Statement(s) => {
            if let ControlFlow::Break(err) = s.visit(&mut visitor) {
                Err(err)
            } else {
                Ok(visitor.relations)
            }
        }
        _ => Ok(visitor.relations),
    }
}

#[test]
fn inserts_version_into_table_ident() {
    let ctx = SessionContext::new();
    let mut stmt = ctx
        .state()
        .sql_to_statement(
            "SELECT * FROM tbl FOR SYSTEM_TIME AS OF '2019-01-17T00:00:00.000Z'",
            "snowflake",
        )
        .unwrap();

    let tables = visit_statement(&mut stmt).unwrap();
    assert_eq!(tables.len(), 1);

    assert_eq!(tables[0].0.to_string(), "tbl".to_string());

    assert_eq!(tables[0].1.to_string(), "tbl__1547683200000".to_string());

    assert_eq!(
        tables[0].2,
        EventTime(Utc.with_ymd_and_hms(2019, 1, 17, 0, 0, 0).unwrap()),
    );
}
