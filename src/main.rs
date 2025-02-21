mod metadata;
mod reader;

use std::str::FromStr;
use chrono::{DateTime, Utc};
use std::{fs, i64};
use std::sync::Arc;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_array::builder::{Int64Builder, StringBuilder};

fn csv_to_parquet(csv: String, parquet: String) {

    let mut keys = Int64Builder::new();
    let mut values = StringBuilder::new();
    let mut timestamps = Int64Builder::new();

    for line in csv.split('\n'){
        let parts: Vec<&str> = line.split(',').collect();
        let key = i64::from_str(parts[0]).unwrap();

        keys.append_value(key);
        values.append_value(parts[1]);
        
        let ts = DateTime::parse_from_rfc3339(parts[2])
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap().timestamp_millis();

        timestamps.append_value(ts);

        //
    }
    let keys_array: Int64Array = keys.finish();
    let values_array: StringArray = values.finish();
    let ts_array : Int64Array = timestamps.finish();

    let schema = Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.into()), vec![
            Arc::new(keys_array),
            Arc::new(values_array),
            Arc::new(ts_array)
    ]);


    println!("OK!");
}

fn main() {

    let csv = fs::read_to_string("test-data/table0/base.csv").unwrap();

    csv_to_parquet(csv, String::from("parquet"));

    println!("Deserialized struct");
}
