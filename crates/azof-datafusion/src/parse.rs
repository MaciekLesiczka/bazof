use azof::AsOf;
use azof::AsOf::{Current, EventTime};
use chrono::{DateTime, Utc};
use datafusion::logical_expr::sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName,
    TableFactor, TableVersion, Value, VisitMut, VisitorMut,
};
use datafusion::sql::parser::Statement;
use std::ops::ControlFlow;

pub struct VersionedTable {
    pub name: ObjectName,
    pub versioned_name: ObjectName,
    pub as_of: AsOf,
}

pub fn rewrite_and_extract_tables(
    statement: &mut Statement,
) -> Result<Vec<VersionedTable>, Box<dyn std::error::Error>> {
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

struct RewriteVersionIntoTableIdent {
    relations: Vec<VersionedTable>,
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
) -> Result<Option<VersionedTable>, Box<dyn std::error::Error>> {
    if let TableFactor::Table { name, version, .. } = table_factor {
        let original_name = name.clone();
        let as_of: Result<AsOf, Box<dyn std::error::Error>> = {
            if let Some(TableVersion::ForSystemTimeAsOf(Expr::Value(Value::SingleQuotedString(
                str,
            )))) = version
            {
                let event_time =
                    DateTime::parse_from_rfc3339(str).map(|dt| dt.with_timezone(&Utc))?;
                let ObjectName(idents) = name;
                let mut new_idents: Vec<Ident> = Vec::with_capacity(idents.len());

                new_idents.extend(idents.iter().take(idents.len() - 1).cloned());

                if let Some(last) = idents.last() {
                    new_idents.push(Ident {
                        value: format!("{}__{}", last.value, event_time.timestamp_millis()),
                        quote_style: last.quote_style,
                        span: last.span,
                    });

                    *name = ObjectName(new_idents);
                    *version = None;
                }
                Ok(EventTime(event_time))
            } else if let Some(TableVersion::Function(Expr::Function(func))) = version {
                if func.name.0.len() == 1 && func.name.0[0].value.to_uppercase() == "AT" {
                    let timestamp_value = extract_timestamp_from_at_function(func)?;
                    let event_time = DateTime::parse_from_rfc3339(&timestamp_value)
                        .map(|dt| dt.with_timezone(&Utc))?;

                    let ObjectName(idents) = name;
                    let mut new_idents: Vec<Ident> = Vec::with_capacity(idents.len());

                    new_idents.extend(idents.iter().take(idents.len() - 1).cloned());

                    if let Some(last) = idents.last() {
                        new_idents.push(Ident {
                            value: format!("{}__{}", last.value, event_time.timestamp_millis()),
                            quote_style: last.quote_style,
                            span: last.span,
                        });

                        *name = ObjectName(new_idents);
                        *version = None;
                    }
                    Ok(EventTime(event_time))
                } else {
                    Ok(Current)
                }
            } else {
                Ok(Current)
            }
        };

        return Ok(Some(VersionedTable {
            name: original_name,
            versioned_name: name.clone(),
            as_of: as_of?,
        }));
    }
    Ok(None)
}

fn extract_timestamp_from_at_function(
    func: &Function,
) -> Result<String, Box<dyn std::error::Error>> {
    if let FunctionArguments::List(list) = &func.args {
        for arg in &list.args {
            match arg {
                FunctionArg::Unnamed(expr) => {
                    if let FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                        timestamp,
                    ))) = expr
                    {
                        return Ok(timestamp.clone());
                    }
                }
                FunctionArg::Named {
                    name,
                    arg,
                    operator: _,
                } => {
                    if name.value.to_uppercase() == "TIMESTAMP" {
                        if let FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                            timestamp,
                        ))) = arg
                        {
                            return Ok(timestamp.clone());
                        }
                    }
                }
                FunctionArg::ExprNamed {
                    name,
                    arg,
                    operator: _,
                } => {
                    if let Expr::Identifier(ident) = name {
                        if ident.value.to_uppercase() == "TIMESTAMP" {
                            if let FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                                timestamp,
                            ))) = arg
                            {
                                return Ok(timestamp.clone());
                            }
                        }
                    }
                }
            }
        }
    }
    Err("No valid timestamp found in AT function".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use datafusion::prelude::SessionContext;

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

        let tables = rewrite_and_extract_tables(&mut stmt).unwrap();
        assert_eq!(tables.len(), 1);

        assert_eq!(tables[0].name.to_string(), "tbl".to_string());

        assert_eq!(
            tables[0].versioned_name.to_string(),
            "tbl__1547683200000".to_string()
        );

        assert_eq!(
            tables[0].as_of,
            EventTime(Utc.with_ymd_and_hms(2019, 1, 17, 0, 0, 0).unwrap()),
        );
    }

    #[test]
    fn handles_at_function_with_unnamed_timestamp() {
        let ctx = SessionContext::new();
        let mut stmt = ctx
            .state()
            .sql_to_statement(
                "SELECT * FROM tbl AT('2019-01-17T00:00:00.000Z')",
                "snowflake",
            )
            .unwrap();

        let tables = rewrite_and_extract_tables(&mut stmt).unwrap();
        assert_eq!(tables.len(), 1);

        assert_eq!(tables[0].name.to_string(), "tbl".to_string());

        assert_eq!(
            tables[0].versioned_name.to_string(),
            "tbl__1547683200000".to_string()
        );

        assert_eq!(
            tables[0].as_of,
            EventTime(Utc.with_ymd_and_hms(2019, 1, 17, 0, 0, 0).unwrap()),
        );
    }

    #[test]
    fn handles_at_function_with_named_timestamp() {
        let ctx = SessionContext::new();
        let mut stmt = ctx
            .state()
            .sql_to_statement(
                "SELECT * FROM tbl AT(TIMESTAMP => '2019-01-17T00:00:00.000Z')",
                "snowflake",
            )
            .unwrap();

        let tables = rewrite_and_extract_tables(&mut stmt).unwrap();
        assert_eq!(tables.len(), 1);

        assert_eq!(tables[0].name.to_string(), "tbl".to_string());

        assert_eq!(
            tables[0].versioned_name.to_string(),
            "tbl__1547683200000".to_string()
        );

        assert_eq!(
            tables[0].as_of,
            EventTime(Utc.with_ymd_and_hms(2019, 1, 17, 0, 0, 0).unwrap()),
        );
    }

    #[test]
    fn returns_error_on_invalid_at_timestamp() {
        let ctx = SessionContext::new();
        let mut stmt = ctx
            .state()
            .sql_to_statement("SELECT * FROM tbl AT('not_a_date')", "snowflake")
            .unwrap();

        let result = rewrite_and_extract_tables(&mut stmt);

        assert!(result.is_err());
    }

    #[test]
    fn returns_error_on_non_convertible_string() {
        let ctx = SessionContext::new();
        let mut stmt = ctx
            .state()
            .sql_to_statement(
                "SELECT * FROM tbl FOR SYSTEM_TIME AS OF 'not_a_date'",
                "snowflake",
            )
            .unwrap();

        let result = rewrite_and_extract_tables(&mut stmt);

        assert!(result.is_err());
    }
}
