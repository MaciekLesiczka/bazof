use arrow::compute::{sort_to_indices, take, SortOptions};
use arrow_array::builder::ArrayBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::RecordBatch;
use bazof::BazofError;
use bazof::{ColumnDef, ColumnType, TableSchema};
use chrono::{DateTime, Utc};
use rand::Rng;
use std::collections::HashSet;
use parquet::column;

pub fn csv_to_arrow(csv: String, schema: TableSchema) -> Result<RecordBatch, BazofError> {
    let (mut keys, mut values, mut timestamps) = schema.column_builders();

    for line in csv.split('\n') {
        let parts: Vec<&str> = line.split(',').collect();

        keys.append_value(parts[0]);

        for i in 0..schema.columns.len() {

            match schema.columns[i].data_type{
                ColumnType::String => {
                    values[i].append_string(parts[i + 1]);
                },
                ColumnType::Int => {
                    values[i].append_int( parts[i + 1].parse::<i64>()?);
                },
                _ => !panic!("Unsupported column type {:?}", schema.columns[i].data_type),
            }
        }

        let ts = DateTime::parse_from_rfc3339(parts[schema.columns.len() + 1])
            .map(|dt| dt.with_timezone(&Utc))?
            .timestamp_millis();

        timestamps.append_value(ts);
    }

    schema.to_batch(keys, timestamps, values)
}

fn _generate_random_batch(
    num_rows: usize,
    ts_range: (i64, i64),
    num_keys: usize,
) -> Result<RecordBatch, BazofError> {
    let mut rng = rand::rng();
    let mut used_pairs = HashSet::new();

    let key_value_schema = TableSchema {
        columns: vec![ColumnDef {
            name: "value".to_string(),
            data_type: ColumnType::String,
            nullable: false,
        }],
    };

    let (mut keys, mut values, mut timestamps) = key_value_schema.column_builders();

    while keys.len() < num_rows {
        let key = rng.random_range(0..num_keys as i64);
        let ts = rng.random_range(ts_range.0..ts_range.1);

        if used_pairs.insert((key, ts)) {
            keys.append_value(key.to_string());
            values[0].append_string(&format!("val_{}", rng.random::<u32>()));
            timestamps.append_value(ts);
        }
    }

    let batch = key_value_schema.to_batch(keys, timestamps, values)?;
    _sort_batch_by_ts_desc(&batch)
}

pub fn print_batch(batch: &RecordBatch) {
    let key_arr = batch.column(0).as_string::<i32>();
    let val_arr = batch.column(1).as_string::<i32>();
    let ts_arr = batch.column(2).as_primitive::<TimestampMillisecondType>();

    for row_idx in 0..batch.num_rows() {
        let key_val = key_arr.value(row_idx);
        let val_val = val_arr.value(row_idx);
        let ts_val = ts_arr.value(row_idx);

        println!(
            "Row {}: Key: {}, Value: {}, Timestamp: {}",
            row_idx, key_val, val_val, ts_val
        );
    }
}

fn _sort_batch_by_ts_desc(batch: &RecordBatch) -> Result<RecordBatch, BazofError> {
    let ts_column = batch.column(2);

    let sort_indices = sort_to_indices(
        ts_column,
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
        None,
    )?;

    let sorted_columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| take(col, &sort_indices, None).unwrap())
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), sorted_columns)?)
}
