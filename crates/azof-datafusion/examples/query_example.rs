use std::path::PathBuf;

use azof_datafusion::context::ExecutionContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut workspace_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    workspace_dir.pop();
    workspace_dir.pop();

    let test_data_path = workspace_dir.join("test-data");

    let absolute_path = test_data_path.canonicalize()?;

    let path_str = absolute_path.to_string_lossy().to_string();

    let ctx = ExecutionContext::new(path_str);

    let df = ctx
        .sql(
            "
    SELECT key as symbol, revenue, net_income
      FROM financials
        AT ('2019-01-17T00:00:00.000Z') -- as per Financial Quarter end date
     WHERE industry IN ('Software')
     ORDER BY revenue DESC
     LIMIT 5;
     ",
        )
        .await?;

    df.show().await?;

    Ok(())
}
