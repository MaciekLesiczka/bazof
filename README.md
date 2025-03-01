# Bazof

Query tables in object storage as of event time.

Bazof is a lakehouse format with time-travel. 

# Project roadmap

Bazof is under development. The goal of to implement data lakehouse with the following capabilities:

* Atomic, non-concurrent writes (single writer)
* Consistent reads
* Schema evolution
* Event time travel queries 
* Handling late-arriving data
* Integration with an execution engine

## Milestone 0

- [x] Script/tool for generating sample kv data set
- [ ] Key-value reader
- [ ] DataFusion table provider

## Milestone 1

 - [ ] Single row, key-value writer
 - [ ] Document spec
 - [ ] Delta -> snapshot compaction
 - [ ] Metadata validity checks

## Milestone 2

- [ ] Schema definition and evolution
- [ ] Late-arriving data support
