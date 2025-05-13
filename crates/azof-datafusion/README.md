# Azof DataFusion Integration

This crate provides integration between Azof lakehouse format and Apache DataFusion.

## Features

- Register Azof tables in a DataFusion context
- Query tables using SQL
- Support for time-travel queries

## Usage

```rust
use azof_datafusion::AzofTableProvider;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use std::sync::Arc;

async fn query_example() -> Result<(), Box<dyn std::error::Error>> {
    // Set up paths and storage
    let store_path = Path::from("/path/to/lakehouse");
    let store = Arc::new(LocalFileSystem::new());
    
    // Create a DataFusion session
    let ctx = SessionContext::new();
    
    // Create a table provider for the current version
    let provider = AzofTableProvider::current(
        store_path.clone(), 
        store.clone(), 
        "my_table".to_string()
    )?;
    
    // Register with DataFusion
    ctx.register_table("my_table", Arc::new(provider))?;
    
    // Execute a SQL query
    let df = ctx.sql("SELECT * FROM my_table WHERE key = '1'").await?;
    df.show().await?;
    
    Ok(())
}
```

## Time-Travel Queries

To query a table as of a specific time:

```rust
use chrono::{TimeZone, Utc};

let event_time = Utc.with_ymd_and_hms(2024, 2, 15, 0, 0, 0).unwrap();
let provider = AzofTableProvider::as_of(
    store_path, 
    store, 
    "my_table".to_string(),
    event_time
)?;
```

## Running the Examples

```bash
cargo run --example query_example -p azof-datafusion
```
