# Azof

[![Rust CI](https://github.com/MaciekLesiczka/azof/actions/workflows/rust.yml/badge.svg)](https://github.com/MaciekLesiczka/azof/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/azof.svg)](https://crates.io/crates/azof)
[![Documentation](https://docs.rs/azof/badge.svg)](https://docs.rs/azof)
[![License](https://img.shields.io/crates/l/azof.svg)](LICENSE)

Query tables in object storage as of event time.

Azof is a lakehouse format with time-travel capabilities that allows you to query data as it existed at any point in time, based on when events actually occurred rather than when they were recorded.


# Practical Example

The `test-data/financials` table contains historical financial information about US public companies, including revenue, income, and share counts. The data is organized by fiscal quarter end dates - when financial results became official - rather than when the data was recorded in the system.

With Azof, you can query this data as it existed at any specific point in time:

```sql
SELECT key as symbol, revenue, net_income
  FROM financials
    AT ('2019-01-17T00:00:00.000Z') -- point-in-time snapshot of available financial data
 WHERE industry IN ('Software')
 ORDER BY revenue DESC
 LIMIT 5
```

This query returns the top 5 software companies by revenue, using only financial data that was officially reported as of January 17, 2019.


## What Problem Does Azof Solve?

Traditional data lakehouse formats allow time travel based on when data was written (processing time). Azof instead focuses on **event time** - the time when events actually occurred in the real world. This distinction is crucial for:

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

The Azof project is organized as a Rust workspace with multiple crates:

- **azof**: The core library providing the lakehouse format functionality
- **azof-cli**: A CLI utility demonstrating how to use the library
- **azof-datafusion**: DataFusion integration for SQL queries

## Getting Started

To build all projects in the workspace:

```bash
cargo build --workspace
```

## Using the CLI

The azof-cli provides a command-line interface for interacting with azof:

```bash
# Scan a table (current version)
cargo run -p azof-cli -- scan --path ./test-data --table table0

# Scan a table as of a specific event time
cargo run -p azof-cli -- scan --path ./test-data --table table0 --as-of "2024-03-15T14:30:00"

# Generate test parquet file from CSV
cargo run -p azof-cli -- gen --path ./test-data --table table2 --file base
```

## DataFusion Integration

The azof-datafusion crate provides integration with Apache DataFusion, allowing you to:

1. Register Azof tables in a DataFusion context
2. Run SQL queries against Azof tables
3. Perform time-travel queries using the AsOf functionality

### Example

```rust
use azof_datafusion::context::ExecutionContext;

async fn query_azof() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = ExecutionContext::new("/path/to/azof");

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
cargo run --example query_example -p azof-datafusion
```

If you install the CLI with `cargo install --path crates/azof-cli`, you can run it directly with:

```bash
azof-cli scan --path ./test-data --table table0
```

## Project Roadmap

Azof is under development. The goal is to implement a data lakehouse with the following capabilities:

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
