use std::path::PathBuf;
use std::sync::Arc;

use bazof_datafusion::BazofTableProvider;
use chrono::{TimeZone, Utc};
use datafusion::prelude::*;
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

    println!("Querying Last 12 months sales for Apple and Google");
    let provider = BazofTableProvider::current(
        store_path.clone(),
        local_store.clone(),
        "ltm_revenue".to_string(),
    )
    .await?;

    ctx.register_table("ltm_revenue", Arc::new(provider))?;

    let df = ctx.sql("SELECT key as symbol, value as revenue FROM ltm_revenue WHERE key IN ('AAPL', 'GOOG') ORDER BY key").await?;
    df.show().await?;

    println!("\nQuerying Last 12 months sales for Apple and Google as of 2019-01-17");
    let event_time = Utc.with_ymd_and_hms(2019, 1, 17, 0, 0, 0).unwrap();

    let provider = BazofTableProvider::as_of(
        store_path.clone(),
        local_store.clone(),
        "ltm_revenue".to_string(),
        event_time,
    )
    .await?;

    // Register the table with DataFusion
    ctx.register_table("ltm_revenue_jan17", Arc::new(provider))?;

    let df = ctx.sql("SELECT key as symbol, value as revenue FROM ltm_revenue_jan17 WHERE key IN ('AAPL', 'GOOG') ORDER BY key").await?;
    df.show().await?;

    Ok(())
}
