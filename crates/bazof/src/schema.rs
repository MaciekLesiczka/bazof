use crate::BazofError;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow_array::builder::{StringBuilder, TimestampMillisecondBuilder};
use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ColumnType {
    Int,
    Float,
    String,
    Boolean,
    Timestamp,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: ColumnType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
}

impl TableSchema {
    pub fn to_arrow_schema(&self) -> Result<SchemaRef, BazofError> {
        let mut fields = Vec::new();

        // Add implicit key field
        fields.push(Field::new("key", DataType::Utf8, false));

        // Add user-defined columns
        for col in &self.columns {
            let arrow_type = match col.data_type {
                ColumnType::Int => DataType::Int64,
                ColumnType::Float => DataType::Float64,
                ColumnType::String => DataType::Utf8,
                ColumnType::Boolean => DataType::Boolean,
                ColumnType::Timestamp => {
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
                }
            };

            fields.push(Field::new(&col.name, arrow_type, col.nullable));
        }

        // Add implicit event_time field
        fields.push(Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ));

        Ok(Arc::new(Schema::new(fields)))
    }
}

pub fn array_builders() -> (StringBuilder, StringBuilder, TimestampMillisecondBuilder) {
    (
        StringBuilder::new(),
        StringBuilder::new(),
        TimestampMillisecondBuilder::new().with_timezone("UTC"),
    )
}

pub fn to_batch(
    mut keys: StringBuilder,
    mut values: StringBuilder,
    mut timestamps: TimestampMillisecondBuilder,
) -> Result<RecordBatch, ArrowError> {
    let array_key = Arc::new(keys.finish());
    let array_value = Arc::new(values.finish());
    let array_ts = Arc::new(timestamps.finish());

    let schema = Arc::new(bazof_schema());
    RecordBatch::try_new(schema, vec![array_key, array_value, array_ts])
}

fn bazof_schema() -> Schema {
    Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_deserialization() {
        let json_str = r#"{
            "columns":[{
                "name":"foo",
                "data_type":"Int",
                "nullable":true
            },
            {
                "name":"bar",
                "data_type":"String",
                "nullable":false
            }]
        }
  "#;
        let table_schema: TableSchema = serde_json::from_str(json_str).unwrap();

        assert_eq!(table_schema.columns.len(), 2);

        assert_eq!(table_schema.columns[0].name, "foo".to_string());
        assert_eq!(table_schema.columns[0].data_type, ColumnType::Int);
        assert_eq!(table_schema.columns[0].nullable, true);

        assert_eq!(table_schema.columns[1].name, "bar".to_string());
        assert_eq!(table_schema.columns[1].data_type, ColumnType::String);
        assert_eq!(table_schema.columns[1].nullable, false);
    }
}
