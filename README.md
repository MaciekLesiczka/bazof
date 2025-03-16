# Bazof

Query tables in object storage as of event time.

Bazof is a lakehouse format with time-travel capabilities.

## Project Structure

The Bazof project is organized as a Rust workspace with multiple crates:

- **bazof**: The core library providing the lakehouse format functionality
- **bazof-cli**: A CLI utility demonstrating how to use the library
- (Future) **bazof-datafusion**: DataFusion integration
- **test-data**: Testing data used by both tests and examples (located at workspace root)

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

If you install the CLI with `cargo install --path crates/bazof-cli`, you can run it directly with:

```bash
bazof-cli scan --path ./test-data --table table0 --as-of "2024-03-15T14:30:00"
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
- [ ] DataFusion table provider

### Milestone 1

 - [ ] Single row, key-value writer
 - [ ] Document spec
 - [ ] Delta -> snapshot compaction
 - [ ] Metadata validity checks

### Milestone 2
- [ ] Streaming in scan
- [ ] Schema definition and evolution
- [ ] Late-arriving data support