use crate::{AzofError, Projection};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_array::builder::{
    BooleanBuilder, Int64Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMillisecondType};
use arrow_array::{ArrayRef, RecordBatch};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const KEY_NAME: &str = "key";
pub const EVENT_TIME_NAME: &str = "event_time";

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

    pub fn append_int(&mut self, data: i64) {
        match self {
            ColumnBuilder::Int(builder) => {
                builder.append_value(data);
            }
            _ => panic!("unexpected column type"),
        }
    }

    pub fn append_boolean(&mut self, data: bool) {
        match self {
            ColumnBuilder::Boolean(builder) => {
                builder.append_value(data);
            }
            _ => panic!("unexpected column type"),
        }
    }

    pub fn append_datetime(&mut self, data: i64) {
        match self {
            ColumnBuilder::DateTime(builder) => {
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
        projection: &Projection,
    ) -> (
        StringBuilder,
        TimestampMillisecondBuilder,
        Vec<ColumnBuilder>,
    ) {
        let mut column_builders: Vec<ColumnBuilder> = vec![];

        for column in &self.columns {
            if projection.contains(&column.name) {
                column_builders.push(ColumnBuilder::new(&column.data_type));
            }
        }
        (
            StringBuilder::new(),
            TimestampMillisecondBuilder::new().with_timezone("UTC"),
            column_builders,
        )
    }

    pub fn to_batch(
        &self,
        mut keys: StringBuilder,
        mut timestamps: TimestampMillisecondBuilder,
        values: Vec<ColumnBuilder>,
        projection: &Projection,
    ) -> Result<RecordBatch, AzofError> {
        let mut columns: Vec<ArrayRef> = vec![];
        if projection.contains(KEY_NAME) {
            columns.push(Arc::new(keys.finish()));
        }

        if projection.contains(EVENT_TIME_NAME) {
            columns.push(Arc::new(timestamps.finish()));
        }

        for mut builder in values {
            columns.push(builder.finish());
        }

        let schema = Arc::new(self.to_arrow_schema(projection)?);

        Ok(RecordBatch::try_new(schema, columns)?)
    }

    pub fn to_arrow_schema(&self, projection: &Projection) -> Result<Schema, AzofError> {
        let mut fields = Vec::new();

        if projection.contains(KEY_NAME) {
            fields.push(Field::new(KEY_NAME, DataType::Utf8, false));
        }

        if projection.contains(EVENT_TIME_NAME) {
            fields.push(Field::new(
                EVENT_TIME_NAME,
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ));
        }

        for col in &self.columns {
            if projection.contains(&col.name) {
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
        }

        Ok(Schema::new(fields))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Projection::All;
    use arrow_array::{Array, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray};
    use chrono::{TimeZone, Utc};

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

        let arrow_schema = table_schema.to_arrow_schema(&All).unwrap();

        assert_eq!(
            arrow_schema,
            Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false,
                ),
                Field::new("foo", DataType::Utf8, true),
                Field::new("bar", DataType::Int64, false),
                Field::new("flag", DataType::Boolean, false),
                Field::new(
                    "created_at",
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    false
                ),
            ])
        );
    }

    #[test]
    fn test_column_builder_creation() {
        let string_builder = ColumnBuilder::new(&ColumnType::String);
        let int_builder = ColumnBuilder::new(&ColumnType::Int);
        let bool_builder = ColumnBuilder::new(&ColumnType::Boolean);
        let date_builder = ColumnBuilder::new(&ColumnType::DateTime);

        match string_builder {
            ColumnBuilder::String(_) => {}
            _ => panic!("Expected String builder"),
        }

        match int_builder {
            ColumnBuilder::Int(_) => {}
            _ => panic!("Expected Int builder"),
        }

        match bool_builder {
            ColumnBuilder::Boolean(_) => {}
            _ => panic!("Expected Boolean builder"),
        }

        match date_builder {
            ColumnBuilder::DateTime(_) => {}
            _ => panic!("Expected DateTime builder"),
        }
    }

    #[test]
    fn test_append_string_to_builder() {
        let mut string_builder = ColumnBuilder::new(&ColumnType::String);

        string_builder.append_string("test1");
        string_builder.append_string("test2");
        string_builder.append_string("test3");

        let array = string_builder.finish();
        let string_array = array.as_string::<i32>();

        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "test1");
        assert_eq!(string_array.value(1), "test2");
        assert_eq!(string_array.value(2), "test3");
    }

    #[test]
    #[should_panic(expected = "unexpected column type")]
    fn test_append_string_to_wrong_builder() {
        let mut int_builder = ColumnBuilder::new(&ColumnType::Int);

        int_builder.append_string("test");

        let mut string_builder = ColumnBuilder::new(&ColumnType::String);

        string_builder.append_int(134);
    }

    #[test]
    fn test_append_value_from_array() {
        let string_array: ArrayRef = Arc::new(StringArray::from(vec!["str1", "str2", "str3"]));
        let int_array: ArrayRef = Arc::new(Int64Array::from(vec![100, 200, 300]));
        let bool_array: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));

        let date1 = Utc
            .with_ymd_and_hms(2023, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        let date2 = Utc
            .with_ymd_and_hms(2023, 2, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        let date3 = Utc
            .with_ymd_and_hms(2023, 3, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        let date_array: ArrayRef =
            Arc::new(TimestampMillisecondArray::from(vec![date1, date2, date3]));

        let mut string_builder = ColumnBuilder::new(&ColumnType::String);
        string_builder.append_value(&string_array, 0);
        string_builder.append_value(&string_array, 2);
        let result = string_builder.finish();
        let result_array = result.as_string::<i32>();
        assert_eq!(result_array.len(), 2);
        assert_eq!(result_array.value(0), "str1");
        assert_eq!(result_array.value(1), "str3");

        let mut int_builder = ColumnBuilder::new(&ColumnType::Int);
        int_builder.append_value(&int_array, 1);
        int_builder.append_value(&int_array, 2);
        let result = int_builder.finish();
        let result_array = result.as_primitive::<Int64Type>();
        assert_eq!(result_array.len(), 2);
        assert_eq!(result_array.value(0), 200);
        assert_eq!(result_array.value(1), 300);

        let mut bool_builder = ColumnBuilder::new(&ColumnType::Boolean);
        bool_builder.append_value(&bool_array, 0);
        bool_builder.append_value(&bool_array, 1);
        bool_builder.append_value(&bool_array, 2);
        let result = bool_builder.finish();
        let result_array = result.as_boolean();
        assert_eq!(result_array.len(), 3);
        assert!(result_array.value(0));
        assert!(!result_array.value(1));
        assert!(result_array.value(2));

        let mut date_builder = ColumnBuilder::new(&ColumnType::DateTime);
        date_builder.append_value(&date_array, 0);
        date_builder.append_value(&date_array, 2);
        let result = date_builder.finish();
        let result_array = result.as_primitive::<TimestampMillisecondType>();
        assert_eq!(result_array.len(), 2);
        assert_eq!(result_array.value(0), date1);
        assert_eq!(result_array.value(1), date3);
    }

    #[test]
    fn test_mixed_column_batch() {
        let schema = TableSchema {
            columns: vec![
                ColumnDef {
                    name: "string_col".to_string(),
                    data_type: ColumnType::String,
                    nullable: false,
                },
                ColumnDef {
                    name: "int_col".to_string(),
                    data_type: ColumnType::Int,
                    nullable: false,
                },
                ColumnDef {
                    name: "bool_col".to_string(),
                    data_type: ColumnType::Boolean,
                    nullable: false,
                },
                ColumnDef {
                    name: "date_col".to_string(),
                    data_type: ColumnType::DateTime,
                    nullable: false,
                },
            ],
        };

        let (mut keys, mut timestamps, mut value_builders) = schema.column_builders(&All);

        keys.append_value("key1");
        keys.append_value("key2");

        let ts1 = Utc
            .with_ymd_and_hms(2023, 1, 10, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        let ts2 = Utc
            .with_ymd_and_hms(2023, 2, 10, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        timestamps.append_value(ts1);
        timestamps.append_value(ts2);

        value_builders[0].append_string("string1");
        value_builders[0].append_string("string2");

        let int_array: ArrayRef = Arc::new(Int64Array::from(vec![100, 200]));
        let bool_array: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        let date1 = Utc
            .with_ymd_and_hms(2023, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        let date2 = Utc
            .with_ymd_and_hms(2023, 2, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        let date_array: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![date1, date2]));

        value_builders[1].append_value(&int_array, 0);
        value_builders[1].append_value(&int_array, 1);

        value_builders[2].append_value(&bool_array, 0);
        value_builders[2].append_value(&bool_array, 1);

        value_builders[3].append_value(&date_array, 0);
        value_builders[3].append_value(&date_array, 1);

        let batch = schema
            .to_batch(keys, timestamps, value_builders, &All)
            .unwrap();

        let arrow_schema = batch.schema();
        assert_eq!(arrow_schema.fields().len(), 6); // key + 4 columns + event_time
        assert_eq!(arrow_schema.field(0).name(), "key");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(1).name(), "event_time");

        assert_eq!(arrow_schema.field(2).name(), "string_col");
        assert_eq!(arrow_schema.field(2).data_type(), &DataType::Utf8);
        assert_eq!(arrow_schema.field(3).name(), "int_col");
        assert_eq!(arrow_schema.field(3).data_type(), &DataType::Int64);
        assert_eq!(arrow_schema.field(4).name(), "bool_col");
        assert_eq!(arrow_schema.field(4).data_type(), &DataType::Boolean);
        assert_eq!(arrow_schema.field(5).name(), "date_col");
        assert_eq!(
            arrow_schema.field(5).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );

        assert_eq!(batch.num_rows(), 2);

        let key_array = batch.column(0).as_string::<i32>();
        assert_eq!(key_array.value(0), "key1");
        assert_eq!(key_array.value(1), "key2");

        let ts_array = batch.column(1).as_primitive::<TimestampMillisecondType>();
        assert_eq!(ts_array.value(0), ts1);
        assert_eq!(ts_array.value(1), ts2);

        let string_array = batch.column(2).as_string::<i32>();
        assert_eq!(string_array.value(0), "string1");
        assert_eq!(string_array.value(1), "string2");

        let int_array = batch.column(3).as_primitive::<Int64Type>();
        assert_eq!(int_array.value(0), 100);
        assert_eq!(int_array.value(1), 200);

        let bool_array = batch.column(4).as_boolean();
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));

        let date_array = batch.column(5).as_primitive::<TimestampMillisecondType>();
        assert_eq!(date_array.value(0), date1);
        assert_eq!(date_array.value(1), date2);
    }
}
