# Azof DataFusion Integration

This crate provides integration between Azof lakehouse format and Apache DataFusion.

It uses SQL dialect that supports table versioning in order to create time-travel queries, like this:

```rust

let df : DataFrame = ctx
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

```

Run:

```bash
cargo run --example query_example -p azof-datafusion
```

and refer to query_example.rs, to see it in action.
