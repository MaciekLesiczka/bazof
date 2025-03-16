# How Bazof Works

Bazof is a lakehouse format that enables event time-travel queries on data stored in object storage. This document explains the core concepts and architecture behind Bazof.

## Core Concepts

### Event Time Travel

Unlike traditional time travel in data lakes that allows querying data based on when it was written (system time), Bazof focuses on **event time** - the time when events actually occurred. This allows for consistent historical views even when data arrives late.

### Base Files

- A **base file** represents a snapshot of data as it existed at a specific point in time (the start time of its segment)
- It contains the complete state of all records within its segment's scope at that specific moment
- Base files provide efficient access to historical states without having to replay all changes

### Segments and Delta Files

Bazof organizes data through a hierarchical structure of segments and delta files:

```
┌───────────────────────────────────────────────────────────────────┐
│                                                                   │
│                         Root Segment                              │
│                       ┌──────────────────┐                        │
│                       │ base10.parquet   │                        │
│                       │ 2022-01 to now   │                        │
│                       │ (snapshot @ 2022-01) │                    │
│                       └─────────┬────────┘                        │
│                                 │                                 │
│         ┌───────────────────────┴───────────────────┐             │
│         │                                           │             │
│  ┌──────┴─────────┐                        ┌────────┴────┴───┐    │
│  │  Segment 11    │                        │   Segment 12    │    │    
│  │ 2022-01 to     │                        │   2023-01 to    │    │    
│  │ 2022-12        │                        │   now           │    │    
│  │ (subrange of parent) │                  │ (subrange of parent) │
│  └──────┬─────────┘                        └────────┬────────┘    │
│         │                                           │             │
│    ┌────┴─────┐                                ┌────┴─────┐       │
│    │ Delta    │                                │ Delta    │       │
│    │ Files    │                                │ Files    │       │
│    └──────────┘                                └──────────┘       │
│  delta_111.parquet                          delta_121.parquet     │
│  delta_112.parquet                          delta_122.parquet     │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

#### Segments

- A **segment** is a logical partition of data, typically covering a specific time range
- Each segment may have:
  - A base file (e.g., `base10.parquet`) containing a snapshot of data at the segment's start time
  - Child segments that each cover a subrange of the parent segment's time period
  - Delta files that contain updates or insertions for that segment's time period
- Each sub-segment represents a more granular view of a subset of its parent's time range
- Segments form a hierarchical tree structure, with each level providing finer time granularity

#### Delta Files

- **Delta files** contain changes (updates, insertions) to records in a segment
- Each delta file has an associated time range (start and end time)
- Multiple delta files in a segment may cover overlapping time periods

### Snapshots

- A **snapshot** is a point-in-time view of the data
- It references the set of segments and delta files that make up a consistent view
- Snapshots are stored as JSON files (e.g., `s1.json`) and referenced by version numbers

## How Queries Work

When you query data "as of" a specific event time:

1. Bazof reads the current version file (`version.txt`) to determine the current snapshot
2. It loads the snapshot file (e.g., `s1.json`) which defines the segment tree
3. It traverses the segment tree, collecting relevant files:
   - Includes base files from segments covering the requested time period (each containing the state at their start time)
   - Includes delta files with start times preceding the requested time
   - Excludes files from segments that don't overlap with the requested time
4. It reads the collected files, keeping the most recent version of each record 
   (based on record key) up to the requested time
5. For overlapping time ranges, data from child segments takes precedence over parent segments

## Example Query Flow

```
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│               │     │               │     │               │
│  Query        │     │  Load         │     │  Traverse     │
│  as-of        ├────►│  Current      ├────►│  Segment      │
│  2022-06-15   │     │  Snapshot     │     │  Tree         │
│               │     │               │     │               │
└───────┬───────┘     └───────────────┘     └───────┬───────┘
        │                                           │
        │                                           ▼
        │                                   ┌───────────────┐
        │                                   │               │
        │                                   │  Collect      │
        │                                   │  Relevant     │
        │                                   │  Files        │
        │                                   │               │
        │                                   └───────┬───────┘
        │                                           │
        ▼                                           ▼
┌───────────────┐                           ┌───────────────┐
│               │                           │ base10.parquet│
│  Return       │◄──────────────────────────┤ delta_111.par │
│  Results      │                           │ delta_112.par │
│               │                           │               │
└───────────────┘                           └───────────────┘
```

## Benefits of This Architecture

1. **Efficient Late-Arriving Data Handling**: New data can be added as delta files without rewriting all data
2. **Hierarchical Time Organization**: Sub-segments provide detailed views of specific time ranges, enabling efficient queries
3. **Point-in-Time Access**: Base files provide direct access to data snapshots at specific points in time
4. **Time-Travel Queries**: Query data as it existed at any point in time
5. **Consistent Views**: Get consistent, repeatable results for historical queries
6. **Scalability**: Works with any object storage (S3, GCS, ADLS, local file system)
7. **Performance**: Only reads the minimum set of files needed for a query

## File Structure Example

```
bazof/
├── table0/
│   ├── version.txt           # Current version pointer "1"
│   ├── s1.json               # Snapshot definition
│   ├── base.parquet          # Base data file
│   └── delta1.parquet        # Delta file with updates
└── table1/
    ├── version.txt
    ├── s1.json
    ├── s2.json               # Newer snapshot
    └── ...
```

## Snapshot Definition Example

```json
{
  "segments": [
    {
      "id": "10",
      "start": "2022-01-01T00:00:00.000Z",
      "file": "base.parquet",  // Snapshot of data as of 2022-01-01
      "delta": [{
        "start": "2022-02-01T00:00:00.000Z",
        "end": "2022-04-01T00:00:00.000Z",
        "file": "delta1.parquet"
      }],
      "segments": [
        {
          "id": "11",
          "start": "2022-03-01T00:00:00.000Z",
          "end": "2022-06-01T00:00:00.000Z",
          "file": "base11.parquet",  // Snapshot of a subrange as of 2022-03-01
          "delta": [
            {
              "start": "2022-04-01T00:00:00.000Z",
              "end": "2022-05-01T00:00:00.000Z",
              "file": "delta11.parquet"
            }
          ]
        }
      ]
    }
  ]
}
```

