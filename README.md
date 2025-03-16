# Bazof

Query tables in object storage as of event time.

Bazof is a lakehouse format with time-travel capabilities.

## Project Structure

The Bazof project is organized as a Rust workspace with multiple crates:

- **bazof**: The core library providing the lakehouse format functionality
- **bazof-cli**: A CLI utility demonstrating how to use the library
- **bazof-datafusion**: DataFusion integration for SQL queries
- **test-data**: Testing data used by both tests and examples (located at workspace root)

## Getting Started

To build all projects in the workspace:

```bash
cargo build --workspace
```

## Using the CLI

The bazof-cli provides a command-line interface for interacting with bazof:

```bash
# Generate test parquet files from CSVs
cargo run -p bazof-cli -- gen

# Scan a table (current version)
cargo run -p bazof-cli -- scan --path /path/to/lakehouse --table table_name

# Scan a table as of a specific event time
cargo run -p bazof-cli -- scan --path /path/to/lakehouse --table table_name --as-of "2024-03-15T14:30:00"
```

## DataFusion Integration

The bazof-datafusion crate provides integration with Apache DataFusion, allowing you to:

1. Register Bazof tables in a DataFusion context
2. Run SQL queries against Bazof tables
3. Perform time-travel queries using the AsOf functionality

### Example

```rust
use bazof_datafusion::BazofTableProvider;
use datafusion::prelude::*;

async fn query_bazof() -> Result<(), Box<dyn std::error::Error>> {
    // Create a DataFusion session
    let ctx = SessionContext::new();
    
    // Create a BazofTableProvider for the current version
    let provider = BazofTableProvider::current(
        store_path, 
        store, 
        "table_name".to_string()
    )?;
    
    // Register the table with DataFusion
    ctx.register_table("my_table", Arc::new(provider))?;
    
    // Run SQL queries
    let df = ctx.sql("SELECT * FROM my_table WHERE key = '1'").await?;
    df.show().await?;
    
    Ok(())
}
```

Run the example:

```bash
cargo run --example query_example -p bazof-datafusion
```

If you install the CLI with `cargo install --path crates/bazof-cli`, you can run it directly with:

```bash
bazof-cli gen
bazof-cli scan --path /path/to/lakehouse --table table_name
```

## Project Roadmap

Bazof is under development. The goal is to implement a data lakehouse with the following capabilities:

* Atomic, non-concurrent writes (single writer)
* Consistent reads
* Schema evolution
* Event time travel queries 
* Handling late-arriving data
* Integration with an execution engine

### Milestone 0

- [x] Script/tool for generating sample kv data set
- [x] Key-value reader
- [x] DataFusion table provider

### Milestone 1

 - [ ] Single row, key-value writer
 - [ ] Document spec
 - [ ] Delta -> snapshot compaction
 - [ ] Metadata validity checks

### Milestone 2
- [ ] Streaming in scan
- [ ] Schema definition and evolution
- [ ] Late-arriving data support