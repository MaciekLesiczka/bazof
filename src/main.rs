extern crate core;

mod metadata;
mod errors;
mod schema;
mod test_bench;
mod table;
mod as_of;
mod lakehouse;

use crate::as_of::AsOf::{Current, EventTime};
use crate::lakehouse::Lakehouse;
use crate::test_bench::{csv_to_arrow, print_batch};
use arrow_array::RecordBatch;
use chrono::{TimeZone, Utc};
use errors::BazofError;
use object_store::local::LocalFileSystem;
use object_store::{path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::sync::Arc;

fn arrow_to_parquet(batch : RecordBatch) -> Result<Vec<u8>, BazofError>{
    let mut buffer = Vec::new();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;

    writer.write(&batch)?;
    writer.close()?;
    Ok(buffer)
}

async fn generate_parquet_test_files() {
    let curr_dir = Path::from(std::env::current_dir().unwrap().to_str().unwrap());
    let local_store = Arc::new(LocalFileSystem::new());
    let lakehouse_dir = curr_dir.child("test-data");


    let csv = fs::read_to_string("test-data/table0/base.csv").unwrap();
    let batch = csv_to_arrow(csv).unwrap();
    let _result = local_store.put(&lakehouse_dir.child("table0").child("base.parquet"),arrow_to_parquet(batch).unwrap().into()).await.unwrap();

    let csv = fs::read_to_string("test-data/table0/delta1.csv").unwrap();
    let batch = csv_to_arrow(csv).unwrap();
    let _result = local_store.put(&lakehouse_dir.child("table0").child("delta1.parquet"),arrow_to_parquet(batch).unwrap().into()).await.unwrap();
}

#[tokio::main]
async fn main() {

    generate_parquet_test_files().await;
    let curr_dir = Path::from(std::env::current_dir().unwrap().to_str().unwrap());
    let local_store = Arc::new(LocalFileSystem::new());
    let lakehouse_dir = curr_dir.child("test-data");

    let lakehouse = Lakehouse::new(lakehouse_dir, local_store);

    let result = lakehouse.scan("table0", Current).await.unwrap();
    println!("Batch read from table0, current:");
    print_batch(&result);


    let past = Utc.with_ymd_and_hms(2024, 3, 1,0, 0, 0).unwrap();
    let result = lakehouse.scan("table0", EventTime(past)).await.unwrap();
    println!("Batch read from lakehouse:");
    print_batch(&result);
}
