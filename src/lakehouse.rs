use crate::as_of::AsOf;
use crate::errors::BazofError;
use crate::table::Table;
use arrow_array::RecordBatch;
use object_store::path::Path;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::sync::Arc;

use crate::as_of::AsOf::EventTime;
use crate::schema::{array_builders, to_batch};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMillisecondType};
use chrono::DateTime;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

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
        let files = table.get_current_data_files(as_of).await?;

        let mut seen: HashMap<i64, (String, i64)> = HashMap::new();

        for file in files {
            let full_path = table.path.child(file);
            let meta = self.store.head(&full_path).await?;
            let reader = ParquetObjectReader::new(self.store.clone(), meta);
            let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;

            let mut parquet_reader = builder.build()?;

            while let Some(mut batch_result) = parquet_reader.next_row_group().await? {
                while let Some(Ok(batch)) = batch_result.next() {
                    let key_arr = batch.column(0).as_primitive::<Int64Type>();
                    let val_arr = batch.column(1).as_string::<i32>();
                    let ts_arr = batch.column(2).as_primitive::<TimestampMillisecondType>();

                    for row_idx in 0..batch.num_rows() {
                        let key_val = key_arr.value(row_idx);

                        if !seen.contains_key(&key_val) {
                            let ts_val = ts_arr.value(row_idx);

                            let ts = DateTime::from_timestamp_millis(ts_val)
                                .ok_or(BazofError::NoneValue)?;
                            if let EventTime(event_time) = as_of {
                                if ts > event_time {
                                    continue;
                                }
                            }
                            let val_val = val_arr.value(row_idx).to_owned();
                            seen.insert(key_val, (val_val, ts_val));
                        }
                    }
                }
            }
        }

        let (mut keys, mut values, mut timestamps) = array_builders();

        for (key, (value, ts)) in seen {
            keys.append_value(key);
            values.append_value(value);
            timestamps.append_value(ts);
        }

        Ok(to_batch(keys,values, timestamps)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::as_of::AsOf::{Current, EventTime};
    use chrono::{TimeZone, Utc};
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    #[tokio::test]
    async fn scan_table() -> Result<(), Box<dyn std::error::Error>> {
        let curr_dir = Path::from(std::env::current_dir()?.to_str().unwrap());
        let local_store = Arc::new(LocalFileSystem::new());
        let lakehouse_dir = curr_dir.child("test-data");
        let lakehouse = Lakehouse::new(lakehouse_dir, local_store);

        let result = lakehouse.scan("table0", Current).await?;
        let result = bazof_batch_to_hash_map(&result);

        let expected: HashMap<i64, String> = HashMap::from([
            (1, "abc2".to_string()),
            (2, "xyz2".to_string()),
            (3, "www2".to_string()),
        ]);

        assert_eq!(result, expected);

        let past = Utc.with_ymd_and_hms(2024, 2, 17, 0, 0, 0).unwrap();
        let result = lakehouse.scan("table0", EventTime(past)).await?;

        let result = bazof_batch_to_hash_map(&result);

        let expected: HashMap<i64, String> =
            HashMap::from([(1, "abc2".to_string()), (2, "xyz".to_string())]);

        assert_eq!(result, expected);

        Ok(())
    }

    fn bazof_batch_to_hash_map(batch : &RecordBatch) -> HashMap<i64, String>{
        let key_array = batch.column(0).as_primitive::<Int64Type>();
        let value_array = batch.column(1).as_string::<i32>();
        let mut result_map: HashMap<i64, String> = HashMap::new();
        for i in 0..key_array.len() {
            result_map.insert(key_array.value(i), value_array.value(i).to_string());
        }
        result_map
    }
}
