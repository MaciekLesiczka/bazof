mod test_bench;

use bazof::AsOf::{Current, EventTime};
use bazof::Lakehouse;
use bazof::BazofError;
use test_bench::{csv_to_arrow, print_batch};
use arrow_array::RecordBatch;
use chrono::{TimeZone, Utc};
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
    let mut workspace_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    workspace_dir.pop();
    workspace_dir.pop();
    
    let test_data_path = workspace_dir.join("test-data");
    let curr_dir = Path::from(test_data_path.to_str().unwrap());
    
    let local_store = Arc::new(LocalFileSystem::new());

    let base_csv_path = workspace_dir.join("test-data").join("table0").join("base.csv");
    let csv = fs::read_to_string(base_csv_path).unwrap();
    let batch = csv_to_arrow(csv).unwrap();
    let _result = local_store.put(&curr_dir.child("table0").child("base.parquet"),arrow_to_parquet(batch).unwrap().into()).await.unwrap();

    let delta_csv_path = workspace_dir.join("test-data").join("table0").join("delta1.csv");
    let csv = fs::read_to_string(delta_csv_path).unwrap();
    let batch = csv_to_arrow(csv).unwrap();
    let _result = local_store.put(&curr_dir.child("table0").child("delta1.parquet"),arrow_to_parquet(batch).unwrap().into()).await.unwrap();
}

#[tokio::main]
async fn main() {
    generate_parquet_test_files().await;
    
    let mut workspace_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    workspace_dir.pop();
    workspace_dir.pop();
    
    let test_data_path = workspace_dir.join("test-data");
    let curr_dir = Path::from(test_data_path.to_str().unwrap());
    
    let local_store = Arc::new(LocalFileSystem::new());
    let lakehouse = Lakehouse::new(curr_dir, local_store);

    let result = lakehouse.scan("table0", Current).await.unwrap();
    println!("Batch read from table0, current:");
    print_batch(&result);


    let past = Utc.with_ymd_and_hms(2024, 3, 1,0, 0, 0).unwrap();
    let result = lakehouse.scan("table0", EventTime(past)).await.unwrap();
    println!("Batch read from lakehouse:");
    print_batch(&result);
}
