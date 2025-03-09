use std::sync::Arc;
use arrow_array::RecordBatch;
use object_store::ObjectStore;
use object_store::path::{Path};
use crate::as_of::AsOf;
use crate::errors::BazofError;
use crate::table::Table;
use std::collections::HashMap;
use crate::schema::bazof_schema;

use arrow::array::{Int64Builder, StringBuilder, TimestampMillisecondBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMillisecondType};

use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

pub struct Lakehouse {
    path: Path,
    store: Arc<dyn ObjectStore>
}

impl Lakehouse {
    pub fn new(path: Path, store: Arc<dyn ObjectStore>) -> Self {
        Lakehouse {
            path,
            store
        }
    }

    pub async fn scan(
        &self,
        table_name: &str,
        as_of: AsOf,
    ) -> Result<RecordBatch, BazofError> {

        let table = Table::new(self.path.child(table_name), self.store.clone());
        let files = table.get_current_data_files(as_of).await?;

        let mut seen: HashMap<i64, (String, i64)> = HashMap::new();

        for file in files {
            let full_path = table.path.child(file);
            let meta = self.store.head(&full_path).await.unwrap();
            let reader = ParquetObjectReader::new(self.store.clone(), meta);
            let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();

            let mut parquet_reader = builder.build()?;

            while let Some(mut batch_result) = parquet_reader.next_row_group().await? {
                while let Some (maybe_batch) = batch_result.next() {
                    let batch = maybe_batch.unwrap();

                    let key_arr = batch.column(0).as_primitive::<Int64Type>();
                    let val_arr = batch.column(1).as_string::<i32>();
                    let ts_arr = batch.column(2).as_primitive::<TimestampMillisecondType>();

                    for row_idx in 0..batch.num_rows() {
                        let key_val = key_arr.value(row_idx);

                        if !seen.contains_key(&key_val) {
                            let val_val = val_arr.value(row_idx).to_owned();
                            let ts_val = ts_arr.value(row_idx);
                            seen.insert(key_val, (val_val, ts_val));
                        }
                    }
                }
            }
        }

        let mut key_builder = Int64Builder::new();
        let mut value_builder = StringBuilder::new();
        let mut ts_builder = TimestampMillisecondBuilder::new().with_timezone("UTC");

        for (key, (value, ts)) in seen {
            key_builder.append_value(key);
            value_builder.append_value(value);
            ts_builder.append_value(ts);
        }

        let array_key = Arc::new(key_builder.finish());
        let array_value = Arc::new(value_builder.finish());
        let array_ts = Arc::new(ts_builder.finish());

        let schema = Arc::new(bazof_schema());
        let batch = RecordBatch::try_new(
            schema,
            vec![array_key, array_value, array_ts],
        )?;

        Ok(batch)
    }
}

