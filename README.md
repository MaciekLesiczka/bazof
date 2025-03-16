# Bazof

Query tables in object storage as of event time.

Bazof is a lakehouse format with time-travel capabilities.

## Project Structure

The Bazof project is organized as a Rust workspace with multiple crates:

- **bazof**: The core library providing the lakehouse format functionality
- **bazof-cli**: A CLI utility demonstrating how to use the library
- (Future) **bazof-datafusion**: DataFusion integration

## Getting Started

To build all projects in the workspace:

```bash
cargo build --workspace
```

To run the CLI example:

```bash
cargo run -p bazof-cli
```

This will create an executable named `bazof-cli`.

If you install the CLI with `cargo install --path crates/bazof-cli`, you can run it directly with:

```bash
bazof-cli
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