
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

pub fn bazof_schema() -> Schema {
    Schema::new(
        vec![
            Field::new("key", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        ])
}