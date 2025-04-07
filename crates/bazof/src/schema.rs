use crate::BazofError;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_array::builder::{StringBuilder, TimestampMillisecondBuilder};
use arrow_array::types::GenericStringType;
use arrow_array::{ArrayRef, GenericByteArray, RecordBatch};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ColumnType {
    String,
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
    fn _to_arrow_schema(&self) -> Result<Schema, BazofError> {
        let mut fields = Vec::new();

        fields.push(Field::new("key", DataType::Utf8, false));

        for col in &self.columns {
            let arrow_type = match col.data_type {
                ColumnType::String => DataType::Utf8,
            };

            fields.push(Field::new(&col.name, arrow_type, col.nullable));
        }

        fields.push(Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ));

        Ok(Schema::new(fields))
    }

    pub fn array_builders(
        &self,
    ) -> (
        StringBuilder,
        Vec<StringBuilder>,
        TimestampMillisecondBuilder,
    ) {
        let mut column_builders: Vec<StringBuilder> = vec![];
        for _ in &self.columns {
            column_builders.push(StringBuilder::new())
        }
        (
            StringBuilder::new(),
            column_builders,
            TimestampMillisecondBuilder::new().with_timezone("UTC"),
        )
    }

    pub fn to_batch(
        &self,
        mut keys: StringBuilder,
        mut timestamps: TimestampMillisecondBuilder,
        values: Vec<GenericByteArray<GenericStringType<i32>>>,
    ) -> Result<RecordBatch, BazofError> {
        let array_key = Arc::new(keys.finish());
        let mut columns: Vec<ArrayRef> = vec![];
        columns.push(array_key);

        for value_array in values {
            columns.push(Arc::new(value_array));
        }

        columns.push(Arc::new(timestamps.finish()));
        let schema = Arc::new(self._to_arrow_schema()?);

        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_deserialization() {
        let json_str = r#"{
            "columns":[{
                "name": "foo",
                "data_type": "String",
                "nullable": true
            },
            {
                "name": "bar",
                "data_type": "String",
                "nullable": false
            }]
        }
  "#;
        let table_schema: TableSchema = serde_json::from_str(json_str).unwrap();

        assert_eq!(table_schema.columns.len(), 2);

        assert_eq!(table_schema.columns[0].name, "foo".to_string());
        assert_eq!(table_schema.columns[0].data_type, ColumnType::String);
        assert!(table_schema.columns[0].nullable);

        assert_eq!(table_schema.columns[1].name, "bar".to_string());
        assert_eq!(table_schema.columns[1].data_type, ColumnType::String);
        assert!(!table_schema.columns[1].nullable);
    }

    #[test]
    fn test_to_arrow_schema() {
        let json_str = r#"{
            "columns":[{
                "name":"foo",
                "data_type":"String",
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

        let arrow_schema = table_schema._to_arrow_schema().unwrap();

        assert_eq!(
            arrow_schema,
            Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("foo", DataType::Utf8, true),
                Field::new("bar", DataType::Utf8, false),
                Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
            ])
        );
    }
}
