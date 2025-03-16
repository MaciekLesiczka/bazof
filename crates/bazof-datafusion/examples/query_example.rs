use std::path::PathBuf;
use std::sync::Arc;

use bazof_datafusion::BazofTableProvider;
use chrono::{TimeZone, Utc};
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use object_store::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build path to test-data at workspace root level
    let mut workspace_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    workspace_dir.pop(); // Go up from crates/bazof-datafusion
    workspace_dir.pop(); // Go up from crates
    
    let test_data_path = workspace_dir.join("test-data");
    println!("Using test data path: {}", test_data_path.display());
    
    let absolute_path = test_data_path.canonicalize()?;
    println!("Absolute path: {}", absolute_path.display());
    
    // Convert to string representation that object_store can use
    let path_str = absolute_path.to_string_lossy().to_string();
    // Make sure it's a valid URI format for object_store
    let path_str = if cfg!(windows) {
        // For Windows, ensure path has the right format
        format!("/{}", path_str.replace("\\", "/"))
    } else {
        // For Unix-like systems
        path_str
    };
    
    println!("Path string for object_store: {}", path_str);
    let store_path = Path::from(path_str);
    
    // Create the local object store
    let local_store = Arc::new(LocalFileSystem::new());
    
    // Create the DataFusion session context
    let ctx = SessionContext::new();
    
    // Example 1: Query the current version of the table
    println!("Querying current version of table0:");
    let provider = BazofTableProvider::current(
        store_path.clone(), 
        local_store.clone(), 
        "table0".to_string()
    )?;
    
    // Register the table with DataFusion
    ctx.register_table("table0_current", Arc::new(provider))?;
    
    // Execute a SQL query
    let df = ctx.sql("SELECT * FROM table0_current ORDER BY key").await?;
    df.show().await?;
    
    // Example 2: Query the table as of a specific time
    println!("\nQuerying table0 as of 2024-02-15:");
    let event_time = Utc.with_ymd_and_hms(2024, 2, 15, 0, 0, 0).unwrap();
    
    let provider = BazofTableProvider::as_of(
        store_path.clone(), 
        local_store.clone(), 
        "table0".to_string(),
        event_time
    )?;
    
    // Register the table with DataFusion
    ctx.register_table("table0_feb15", Arc::new(provider))?;
    
    // Execute a SQL query
    let df = ctx.sql("SELECT * FROM table0_feb15 ORDER BY key").await?;
    df.show().await?;
    
    // Example 3: More complex query with projection and filtering
    println!("\nQuerying with projection and filtering:");
    let df = ctx.sql("SELECT key, value FROM table0_current WHERE key = '2'").await?;
    df.show().await?;
    
    Ok(())
}