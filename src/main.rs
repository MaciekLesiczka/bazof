extern crate core;

mod metadata;
mod reader;
mod errors;
mod schema;
mod test_bench;
mod table;
mod as_of;

use arrow_array::RecordBatch;
use errors::BazofError;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use std::fs;


use crate::test_bench::{csv_to_arrow, generate_random_batch};
use object_store::local::LocalFileSystem;
use object_store::{path::Path, ObjectStore};

fn arrow_to_parquet(batch : RecordBatch) -> Result<Vec<u8>, BazofError>{
    let mut buffer = Vec::new();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;

    writer.write(&batch)?;
    writer.close()?;
    Ok(buffer)
}

#[tokio::main]
async fn main() {

    let b = generate_random_batch(20, (1700000000000, 1705000000000), 5).unwrap();

    //let store = Arc::new(InMemory::new());
    let store = Arc::new(LocalFileSystem::new());
    let path = Path::from("/home/maciek/src/bazof/test-data/table0/base_rand.parquet");

    let csv = fs::read_to_string("test-data/table0/base.csv").unwrap();

    let _batch = csv_to_arrow(csv).unwrap();

    let buffer = arrow_to_parquet(b).unwrap();

    let _result = store.put(&path,buffer.into()).await.unwrap();

    println!("Put csv to object store");
}
