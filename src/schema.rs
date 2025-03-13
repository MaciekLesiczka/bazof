use std::sync::Arc;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow_array::builder::{Int64Builder, StringBuilder, TimestampMillisecondBuilder};
use arrow_array::RecordBatch;


pub fn array_builders() -> (Int64Builder, StringBuilder, TimestampMillisecondBuilder) {
    (Int64Builder::new(),
     StringBuilder::new(),
     TimestampMillisecondBuilder::new().with_timezone("UTC"))
}

pub fn to_batch(mut keys: Int64Builder,mut values: StringBuilder, mut timestamps: TimestampMillisecondBuilder) -> Result<RecordBatch, ArrowError> {
    let array_key = Arc::new(keys.finish());
    let array_value = Arc::new(values.finish());
    let array_ts = Arc::new(timestamps.finish());

    let schema = Arc::new(bazof_schema());
    RecordBatch::try_new(schema, vec![array_key, array_value, array_ts])
}

fn bazof_schema() -> Schema {
    Schema::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        ])
}