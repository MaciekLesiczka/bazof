extern crate core;

mod metadata;
mod scanner;
mod errors;
mod schema;
mod test_bench;
mod table;
mod as_of;
mod lakehouse;

use arrow_array::RecordBatch;
use errors::BazofError;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use std::fs;
use parquet::arrow::async_reader::ParquetObjectReader;
use arrow_array::cast::AsArray;
use crate::test_bench::{csv_to_arrow, generate_random_batch, print_batch};
use object_store::{path::Path, ObjectStore};
use object_store::memory::InMemory;

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

    let csv = fs::read_to_string("test-data/table0/base.csv").unwrap();

    let batch = csv_to_arrow(csv).unwrap();

    println!("Batch converted from CSV:");

    print_batch(&batch);

    let store = Arc::new(InMemory::new());

    let b = generate_random_batch(5, (1700000000000, 1705000000000), 2).unwrap();

    let path = Path::from("base_rand.parquet");

    let buffer = arrow_to_parquet(b).unwrap();

    let _result = store.put(&path,buffer.into()).await.unwrap();

    let meta = store.head(&path).await.unwrap();
    let reader = ParquetObjectReader::new(store, meta);

    let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
    let mut stream = builder.build().unwrap();

    while let Some(mut batch_result) = stream.next_row_group().await.unwrap(){
        while let Some (maybe_batch) = batch_result.next() {
            let batch = maybe_batch.unwrap();
            println!("Batch read from store:");
            print_batch(&batch)
        }
    }
}
