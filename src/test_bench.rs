use crate::errors::BazofError;
use crate::schema::{array_builders, to_batch};
use arrow::compute::{sort_to_indices, take, SortOptions};
use arrow_array::builder::ArrayBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{TimestampMillisecondType};
use arrow_array::RecordBatch;
use chrono::{DateTime, Utc};
use rand::Rng;
use std::collections::HashSet;


pub fn csv_to_arrow(csv: String) -> Result<RecordBatch, BazofError> {
    let (mut keys, mut values, mut timestamps) = array_builders();

    for line in csv.split('\n'){
        let parts: Vec<&str> = line.split(',').collect();

        keys.append_value(parts[0]);
        values.append_value(parts[1]);

        let ts = DateTime::parse_from_rfc3339(parts[2])
            .map(|dt| dt.with_timezone(&Utc))
            ?.timestamp_millis();

        timestamps.append_value(ts);
    }

    Ok(to_batch(keys,values, timestamps)?)

}

fn _generate_random_batch(num_rows: usize, ts_range: (i64, i64), num_keys: usize) -> Result<RecordBatch, BazofError> {
    let mut rng = rand::rng();
    let mut used_pairs = HashSet::new();

    let (mut keys, mut values, mut timestamps) = array_builders();

    while keys.len() < num_rows {
        let key = rng.random_range(0..num_keys as i64);
        let ts = rng.random_range(ts_range.0..ts_range.1);

        if used_pairs.insert((key, ts)) {
            keys.append_value(key.to_string());
            values.append_value(format!("val_{}", rng.random::<u32>()));
            timestamps.append_value(ts);
        }
    }

    let batch = to_batch(keys,values, timestamps)?;

    Ok(_sort_batch_by_ts_desc(&batch)?)
}

pub fn print_batch(batch: &RecordBatch) -> () {
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

    let sort_indices = sort_to_indices(ts_column, Some(SortOptions { descending: true, nulls_first: false }), None)?;

    let sorted_columns: Vec<_> = batch.columns()
        .iter()
        .map(|col| take(col, &sort_indices, None).unwrap())
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), sorted_columns)?)
}
