use crate::as_of::AsOf;
use crate::as_of::AsOf::EventTime;
use crate::errors::BazofError;
use crate::schema::TableSchema;
use crate::table::Table;
use arrow_array::cast::AsArray;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::RecordBatch;
use chrono::DateTime;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Lakehouse {
    path: Path,
    store: Arc<dyn ObjectStore>,
}

impl Lakehouse {
    pub fn new(path: Path, store: Arc<dyn ObjectStore>) -> Self {
        Lakehouse { path, store }
    }

    pub async fn scan(&self, table_name: &str, as_of: AsOf) -> Result<RecordBatch, BazofError> {
        let table = Table::new(self.path.child(table_name), self.store.clone());
        let snapshot = table.get_current_snapshot().await?;
        let files = snapshot.get_data_files(as_of);
        let schema = snapshot.schema;

        let mut seen: HashMap<String, i64> = HashMap::new();

        let (mut keys, mut values, mut timestamps) = schema.array_builders();

        for file in files {
            let full_path = table.path.child(file);
            let meta = self.store.head(&full_path).await?;
            let reader = ParquetObjectReader::new(self.store.clone(), meta);
            let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;

            let mut parquet_reader = builder.build()?;

            while let Some(mut batch_result) = parquet_reader.next_row_group().await? {
                while let Some(Ok(batch)) = batch_result.next() {
                    let key_arr = batch.column(0).as_string::<i32>();

                    let mut column_arrays = vec![];
                    for col_idx in 1..schema.columns.len() + 1 {
                        let column_array = batch.column(col_idx).as_string::<i32>();
                        column_arrays.push(column_array);
                    }

                    let ts_arr = batch
                        .column(batch.num_columns() - 1)
                        .as_primitive::<TimestampMillisecondType>();

                    for row_idx in 0..batch.num_rows() {
                        let key_val = key_arr.value(row_idx);

                        if let std::collections::hash_map::Entry::Vacant(e) =
                            seen.entry(key_val.to_owned())
                        {
                            let ts_val = ts_arr.value(row_idx);

                            let ts = DateTime::from_timestamp_millis(ts_val)
                                .ok_or(BazofError::NoneValue)?;
                            if let EventTime(event_time) = as_of {
                                if ts > event_time {
                                    continue;
                                }
                            }

                            e.insert(ts_val);

                            for i in 0..schema.columns.len() {
                                let val_arr = column_arrays[i];
                                let val_val = val_arr.value(row_idx);
                                values[i].append_value(val_val);
                            }
                            keys.append_value(key_val);

                            timestamps.append_value(ts_val);
                        }
                    }
                }
            }
        }

        let mut value_arrays = vec![];
        for builder in &mut values {
            value_arrays.push(builder.finish());
        }
        schema.to_batch(keys, timestamps, value_arrays)
    }

    pub async fn get_schema(&self, table_name: &str) -> Result<TableSchema, BazofError> {
        let table = Table::new(self.path.child(table_name), self.store.clone());
        Ok(table.get_current_snapshot().await?.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::as_of::AsOf::{Current, EventTime};
    use arrow_array::Array;
    use chrono::{TimeZone, Utc};
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use std::path::PathBuf;

    #[tokio::test]
    async fn scan_table_with_one_segment_and_delta() -> Result<(), Box<dyn std::error::Error>> {
        let mut workspace_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        workspace_dir.pop();
        workspace_dir.pop();

        let test_data_path = workspace_dir.join("test-data");
        let curr_dir = Path::from(test_data_path.to_str().unwrap());

        let local_store = Arc::new(LocalFileSystem::new());
        let lakehouse = Lakehouse::new(curr_dir, local_store);

        let result = lakehouse.scan("table0", Current).await?;
        let result = bazof_batch_to_hash_map(&result);

        let expected: HashMap<String, String> = HashMap::from([
            (1.to_string(), "abc2".to_string()),
            (2.to_string(), "xyz2".to_string()),
            (3.to_string(), "www2".to_string()),
        ]);

        assert_eq!(result, expected);

        let past = Utc.with_ymd_and_hms(2024, 2, 17, 0, 0, 0).unwrap();
        let result = lakehouse.scan("table0", EventTime(past)).await?;

        let result = bazof_batch_to_hash_map(&result);

        let expected: HashMap<String, String> = HashMap::from([
            (1.to_string(), "abc2".to_string()),
            (2.to_string(), "xyz".to_string()),
        ]);

        assert_eq!(result, expected);

        Ok(())
    }

    #[tokio::test]
    async fn scan_table_with_delta_multiple_updates() -> Result<(), Box<dyn std::error::Error>> {
        let mut workspace_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        workspace_dir.pop();
        workspace_dir.pop();

        let test_data_path = workspace_dir.join("test-data");
        let curr_dir = Path::from(test_data_path.to_str().unwrap());

        let local_store = Arc::new(LocalFileSystem::new());
        let lakehouse = Lakehouse::new(curr_dir, local_store);

        let result = lakehouse.scan("table1", Current).await?;
        let result = bazof_batch_to_hash_map(&result);

        let expected: HashMap<String, String> = HashMap::from([
            (1.to_string(), "abc4".to_string()),
            (2.to_string(), "xyz3".to_string()),
        ]);

        assert_eq!(result, expected);

        let past = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
        let result = lakehouse.scan("table1", EventTime(past)).await?;
        let result = bazof_batch_to_hash_map(&result);

        let expected: HashMap<String, String> = HashMap::from([
            (1.to_string(), "abc3".to_string()),
            (2.to_string(), "xyz2".to_string()),
        ]);

        assert_eq!(result, expected);

        let past = Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap();
        let result = lakehouse.scan("table1", EventTime(past)).await?;
        let result = bazof_batch_to_hash_map(&result);

        let expected: HashMap<String, String> =
            HashMap::from([(1.to_string(), "abc2".to_string())]);

        assert_eq!(result, expected);

        Ok(())
    }

    #[tokio::test]
    async fn scan_table_with_one_segment_and_delta_with_multiple_columns(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut workspace_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        workspace_dir.pop();
        workspace_dir.pop();

        let test_data_path = workspace_dir.join("test-data");
        let curr_dir = Path::from(test_data_path.to_str().unwrap());

        let local_store = Arc::new(LocalFileSystem::new());
        let lakehouse = Lakehouse::new(curr_dir, local_store);

        let result = lakehouse.scan("table2", Current).await?;
        let result = bazof_batch_to_hash_map_2columns(&result);

        let expected: HashMap<String, (String, String)> = HashMap::from([
            (1.to_string(), ("abc2".to_string(), "II_abc".to_string())),
            (2.to_string(), ("xyz2".to_string(), "II_xyz".to_string())),
            (3.to_string(), ("www2".to_string(), "II_www2".to_string())),
        ]);

        assert_eq!(result, expected);

        let past = Utc.with_ymd_and_hms(2024, 2, 17, 0, 0, 0).unwrap();
        let result = lakehouse.scan("table2", EventTime(past)).await?;

        let result = bazof_batch_to_hash_map_2columns(&result);

        let expected: HashMap<String, (String, String)> = HashMap::from([
            (1.to_string(), ("abc2".to_string(), "II_abc".to_string())),
            (2.to_string(), ("xyz".to_string(), "II_xyz".to_string())),
        ]);

        assert_eq!(result, expected);

        Ok(())
    }

    fn bazof_batch_to_hash_map(batch: &RecordBatch) -> HashMap<String, String> {
        let key_array = batch.column(0).as_string::<i32>();
        let value_array = batch.column(1).as_string::<i32>();
        let mut result_map: HashMap<String, String> = HashMap::new();
        for i in 0..key_array.len() {
            result_map.insert(
                key_array.value(i).to_owned(),
                value_array.value(i).to_owned(),
            );
        }
        result_map
    }

    fn bazof_batch_to_hash_map_2columns(batch: &RecordBatch) -> HashMap<String, (String, String)> {
        let key_array = batch.column(0).as_string::<i32>();
        let value_array = batch.column(1).as_string::<i32>();
        let value2_array = batch.column(2).as_string::<i32>();
        let mut result_map: HashMap<String, (String, String)> = HashMap::new();
        for i in 0..key_array.len() {
            result_map.insert(
                key_array.value(i).to_owned(),
                (
                    value_array.value(i).to_owned(),
                    value2_array.value(i).to_owned(),
                ),
            );
        }
        result_map
    }
}
