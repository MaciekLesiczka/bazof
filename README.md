# Bazof

[![Rust CI](https://github.com/MaciekLesiczka/bazof/actions/workflows/rust.yml/badge.svg)](https://github.com/MaciekLesiczka/bazof/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/bazof.svg)](https://crates.io/crates/bazof)
[![Documentation](https://docs.rs/bazof/badge.svg)](https://docs.rs/bazof)
[![License](https://img.shields.io/crates/l/bazof.svg)](LICENSE)

Query tables in object storage as of event time.

Bazof is a lakehouse format with time-travel capabilities that allows you to query data as it existed at any point in time, based on when events actually occurred rather than when they were recorded.

## What Problem Does Bazof Solve?

Traditional data lakehouse formats allow time travel based on when data was written (processing time). Bazof instead focuses on **event time** - the time when events actually occurred in the real world. This distinction is crucial for:

- **Late-arriving data**: Process data that arrives out of order without rewriting history
- **Consistent historical views**: Get consistent snapshots of data as it existed at specific points in time
- **High cardinality datasets with frequent updates**: Efficiently handle use cases involving business processes (sales, support, project management, financial data) or slowly changing dimensions
- **Point-in-time analysis**: Analyze the state of your data exactly as it was at any moment

## Key Features

- **Event time-based time travel**: Query data based on when events occurred, not when they were recorded
- **Efficient storage of updates**: Preserves compacted snapshots of state to minimize storage and query overhead
- **Hierarchical organization**: Uses segments and delta files to efficiently organize temporal data
- **Tunable compaction policy**: Adjust based on your data distribution patterns
- **SQL integration**: Query using DataFusion with familiar SQL syntax
- **Integration with object storage**: Works with any object store (local, S3, etc.)


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

# Generate test parquet file from CSV
cargo run -p bazof-cli -- gen --path ./test-data --table table2 --file base
```

## DataFusion Integration

The bazof-datafusion crate provides integration with Apache DataFusion, allowing you to:

1. Register Bazof tables in a DataFusion context
2. Run SQL queries against Bazof tables
3. Perform time-travel queries using the AsOf functionality

### Example

```rust
use bazof_datafusion::context::ExecutionContext;

async fn query_bazof() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = ExecutionContext::new("/path/to/bazof");

    let df = ctx
        .sql(
            "
    SELECT key as symbol, revenue, net_income
      FROM financials
        AT ('2019-01-17T00:00:00.000Z')
     WHERE industry IN ('Software')
     ORDER BY revenue DESC
     LIMIT 5;
     ",
        )
        .await?;

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

 - [x] Multiple columns support
 - [x] Data Types columns support
 - [x] Projection pushdown
 - [x] Projection pushdown in DataFusion table provider
 - [x] DataFusion table provider with AS OF operator
 - [ ] Single row, key-value writer
 - [ ] Document spec
 - [ ] Delta -> snapshot compaction
 - [ ] Metadata validity checks

### Milestone 2
- [ ] Streaming in scan
- [ ] Schema definition and evolution
- [ ] Late-arriving data support
