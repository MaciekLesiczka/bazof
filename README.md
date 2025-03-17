# Bazof

[![Rust CI](https://github.com/MaciekLesiczka/bazof/actions/workflows/rust.yml/badge.svg)](https://github.com/MaciekLesiczka/bazof/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/bazof.svg)](https://crates.io/crates/bazof)
[![Documentation](https://docs.rs/bazof/badge.svg)](https://docs.rs/bazof)
[![License](https://img.shields.io/crates/l/bazof.svg)](LICENSE)

Query tables in object storage as of event time.

Bazof is a lakehouse format with time-travel capabilities.

## Project Structure

The Bazof project is organized as a Rust workspace with multiple crates:

- **bazof**: The core library providing the lakehouse format functionality
- **bazof-cli**: A CLI utility demonstrating how to use the library
- **bazof-datafusion**: DataFusion integration for SQL queries

## Getting Started

To build all projects in the workspace:

```bash
cargo build --workspace
```

## Using the CLI

The bazof-cli provides a command-line interface for interacting with bazof:

```bash
# Scan a table (current version)
cargo run -p bazof-cli -- scan --path ./test-data --table table0

# Scan a table as of a specific event time
cargo run -p bazof-cli -- scan --path ./test-data --table table0 --as-of "2024-03-15T14:30:00"
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
    let ctx = SessionContext::new();
    
    let event_time = Utc.with_ymd_and_hms(2019, 1, 17, 0, 0, 0).unwrap();
    
    let provider = BazofTableProvider::as_of(
        store_path.clone(), 
        local_store.clone(), 
        "ltm_revenue".to_string(),
        event_time
    )?;
    
    ctx.register_table("ltm_revenue_jan17", Arc::new(provider))?;
    
    let df = ctx.sql("SELECT key as symbol, value as revenue FROM ltm_revenue_jan17 WHERE key IN ('AAPL', 'GOOG') ORDER BY key").await?;
    df.show().await?;
}
```

Run the example:

```bash
cargo run --example query_example -p bazof-datafusion
```

If you install the CLI with `cargo install --path crates/bazof-cli`, you can run it directly with:

```bash
bazof-cli scan --path ./test-data --table table0
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

 - [ ] Multiple columns support
 - [ ] Single row, key-value writer
 - [ ] Document spec
 - [ ] Delta -> snapshot compaction
 - [ ] Metadata validity checks

### Milestone 2
- [ ] Streaming in scan
- [ ] Schema definition and evolution
- [ ] Late-arriving data support

