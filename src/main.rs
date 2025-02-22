mod metadata;
mod reader;

use std::str::FromStr;
use chrono::{DateTime, ParseError, Utc};
use std::{fs, i64};
use std::sync::Arc;
use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow_array::builder::{Int64Builder, StringBuilder, TimestampMillisecondBuilder};
use parquet::arrow::{ArrowWriter};
use thiserror::Error;
use parquet::file::properties::WriterProperties;

#[derive(Debug, Error)]
enum BazofError {
    #[error("IO error: {0}")]
    Arrow(#[from] ArrowError),
    #[error("Parsing error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("DateTime parsing error: {0}")]
    ParseChrono(#[from] ParseError),
}

fn csv_to_arrow(csv: String) -> Result<RecordBatch, BazofError> {

    let mut keys = Int64Builder::new();
    let mut values = StringBuilder::new();
    let mut timestamps = TimestampMillisecondBuilder::new();

    for line in csv.split('\n'){
        let parts: Vec<&str> = line.split(',').collect();
        let key = i64::from_str(parts[0])?;

        keys.append_value(key);
        values.append_value(parts[1]);
        
        let ts = DateTime::parse_from_rfc3339(parts[2])
        .map(|dt| dt.with_timezone(&Utc))
        ?.timestamp_millis();

        timestamps.append_value(ts);
    }
    let keys_array: Int64Array = keys.finish();
    let values_array: StringArray = values.finish();
    let ts_array : TimestampMillisecondArray = timestamps.finish();

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
    ])?;

    Ok(batch)
}

use object_store::memory::InMemory;
use object_store::{path::Path, ObjectStore};

#[tokio::main]
async fn main() {

    let store = Arc::new(InMemory::new());
    let path = Path::from("test.parquet");

    let csv = fs::read_to_string("test-data/table0/base.csv").unwrap();

    let batch = csv_to_arrow(csv).unwrap();

    let mut buffer = Vec::new();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let result = store.put(&path,buffer.into()).await.unwrap();

    println!("Deserialized struct");
}
