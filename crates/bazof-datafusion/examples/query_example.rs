use std::path::PathBuf;

use bazof_datafusion::context::ExecutionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut workspace_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    workspace_dir.pop();
    workspace_dir.pop();

    let test_data_path = workspace_dir.join("test-data");

    let absolute_path = test_data_path.canonicalize()?;

    let path_str = absolute_path.to_string_lossy().to_string();
    
    let ctx = ExecutionContext::new(path_str);

    let df = ctx.sql("
    SELECT key as symbol, value as revenue
      FROM ltm_revenue FOR SYSTEM_TIME AS OF '2019-01-17T00:00:00.000Z'
     WHERE key IN ('AAPL', 'GOOG')
     ORDER BY key").await?;

    df.show().await?;

    Ok(())
}
