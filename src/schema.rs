use std::sync::Arc;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow_array::builder::{StringBuilder, TimestampMillisecondBuilder};
use arrow_array::RecordBatch;


pub fn array_builders() -> (StringBuilder, StringBuilder, TimestampMillisecondBuilder) {
    (StringBuilder::new(),
     StringBuilder::new(),
     TimestampMillisecondBuilder::new().with_timezone("UTC"))
}

pub fn to_batch(mut keys: StringBuilder,mut values: StringBuilder, mut timestamps: TimestampMillisecondBuilder) -> Result<RecordBatch, ArrowError> {
    let array_key = Arc::new(keys.finish());
    let array_value = Arc::new(values.finish());
    let array_ts = Arc::new(timestamps.finish());

    let schema = Arc::new(bazof_schema());
    RecordBatch::try_new(schema, vec![array_key, array_value, array_ts])
}

fn bazof_schema() -> Schema {
    Schema::new(
        vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("event_time", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        ])
}