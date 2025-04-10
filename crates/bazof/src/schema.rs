use crate::BazofError;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_array::builder::{
    BooleanBuilder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMillisecondType};
use arrow_array::{ArrayRef, RecordBatch};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum ColumnType {
    String,
    Int,
    Boolean,
    DateTime,
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

pub enum ColumnBuilder {
    String(StringBuilder),
    Int(Int64Builder),
    Boolean(BooleanBuilder),
    DateTime(TimestampMillisecondBuilder),
}

impl ColumnBuilder {
    pub fn new(column_type: &ColumnType) -> Self {
        match column_type {
            ColumnType::String => ColumnBuilder::String(StringBuilder::new()),
            ColumnType::Int => ColumnBuilder::Int(Int64Builder::new()),
            ColumnType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::new()),
            ColumnType::DateTime => {
                ColumnBuilder::DateTime(TimestampMillisecondBuilder::new().with_timezone("UTC"))
            }
        }
    }

    pub fn append_value(&mut self, array: &ArrayRef, row_idx: usize) {
        match self {
            ColumnBuilder::String(builder) => {
                let val_arr = array.as_string::<i32>();
                let val_val = val_arr.value(row_idx);
                builder.append_value(val_val);
            }
            ColumnBuilder::Int(builder) => {
                let val_arr = array.as_primitive::<Int64Type>();
                let val_val = val_arr.value(row_idx);
                builder.append_value(val_val);
            }
            ColumnBuilder::Boolean(builder) => {
                let val_arr = array.as_boolean();
                let val_val = val_arr.value(row_idx);
                builder.append_value(val_val);
            }
            ColumnBuilder::DateTime(builder) => {
                let val_arr = array.as_primitive::<TimestampMillisecondType>();
                let val_val = val_arr.value(row_idx);
                builder.append_value(val_val);
            }
        }
    }

    pub fn append_string(&mut self, data: &str) {
        match self {
            ColumnBuilder::String(builder) => {
                builder.append_value(data);
            }
            _ => panic!("unexpected column type"),
        }
    }

    pub fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::String(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Boolean(builder) => Arc::new(builder.finish()),
            ColumnBuilder::DateTime(builder) => Arc::new(builder.finish()),
        }
    }
}

impl TableSchema {
    pub fn column_builders(
        &self,
    ) -> (
        StringBuilder,
        Vec<ColumnBuilder>,
        TimestampMillisecondBuilder,
    ) {
        let mut column_builders: Vec<ColumnBuilder> = vec![];
        for column in &self.columns {
            column_builders.push(ColumnBuilder::new(&column.data_type));
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
        values: Vec<ColumnBuilder>,
    ) -> Result<RecordBatch, BazofError> {
        let array_key = Arc::new(keys.finish());
        let mut columns: Vec<ArrayRef> = vec![];
        columns.push(array_key);

        for mut builder in values {
            columns.push(builder.finish());
        }

        columns.push(Arc::new(timestamps.finish()));
        let schema = Arc::new(self.to_arrow_schema()?);

        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn to_arrow_schema(&self) -> Result<Schema, BazofError> {
        let mut fields = Vec::new();

        fields.push(Field::new("key", DataType::Utf8, false));

        for col in &self.columns {
            let arrow_type = match col.data_type {
                ColumnType::String => DataType::Utf8,
                ColumnType::Int => DataType::Int64,
                ColumnType::Boolean => DataType::Boolean,
                ColumnType::DateTime => {
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
                }
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
                "data_type":"Int",
                "nullable":false
            },
            {
                "name":"flag",
                "data_type":"Boolean",
                "nullable":false
            },
            {
                "name":"created_at",
                "data_type":"DateTime",
                "nullable":false
            }]
        }
  "#;
        let table_schema: TableSchema = serde_json::from_str(json_str).unwrap();

        let arrow_schema = table_schema.to_arrow_schema().unwrap();

        assert_eq!(
            arrow_schema,
            Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("foo", DataType::Utf8, true),
                Field::new("bar", DataType::Int64, false),
                Field::new("flag", DataType::Boolean, false),
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false
                ),
                Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
            ])
        );
    }
}
