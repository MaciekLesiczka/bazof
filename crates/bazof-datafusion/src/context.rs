use crate::parse::rewrite_and_extract_tables;
use crate::BazofTableProvider;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::SessionContext;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::ObjectStore;
use std::error::Error;
use std::sync::Arc;

pub struct ExecutionContext {
    ctx: SessionContext,
    store_path: Path,
    store: Arc<dyn ObjectStore>,
}

impl ExecutionContext {
    pub fn new(path: String) -> ExecutionContext {
        ExecutionContext {
            ctx: SessionContext::new(),
            store_path: Path::from(path),
            store: Arc::new(LocalFileSystem::new()),
        }
    }

    pub async fn sql(&self, sql: &str) -> Result<DataFrame, Box<dyn Error>> {
        let mut stmt = self.ctx.state().sql_to_statement(sql, "snowflake")?;
        let tables = rewrite_and_extract_tables(&mut stmt)?;

        for versioned_table in tables {
            let table_ref = versioned_table.versioned_name.to_string();
            let provider = BazofTableProvider::new(
                self.store_path.clone(),
                self.store.clone(),
                versioned_table.name.to_string(),
                versioned_table.as_of,
            )
            .await?;

            if !self.ctx.table_exist(&table_ref)? {
                self.ctx.register_table(table_ref, Arc::new(provider))?;
            }
        }

        let plan = self.ctx.state().statement_to_plan(stmt).await?;
        Ok(self.ctx.execute_logical_plan(plan).await?)
    }
}
