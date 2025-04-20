extern crate core;

mod test_bench;

use arrow_array::RecordBatch;
use bazof::AsOf::{Current, EventTime};
use bazof::BazofError;
use bazof::Lakehouse;
use bazof::Projection::All;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use object_store::local::LocalFileSystem;
use object_store::{path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use test_bench::{csv_to_arrow, print_batch};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate test parquet files from CSV files for given table. Infers the schema from snapshot file
    Gen {
        /// Path to lakehouse directory
        #[arg(short, long)]
        path: PathBuf,

        /// Table name
        #[arg(short, long)]
        table: String,

        /// Filename, without extension
        #[arg(short, long)]
        file: String,
    },

    /// Scan a table in a lakehouse directory
    Scan {
        /// Path to lakehouse directory
        #[arg(short, long)]
        path: PathBuf,

        /// Table name to scan
        #[arg(short, long)]
        table: String,

        /// Optional timestamp for as-of queries (format: YYYY-MM-DDTHH:mm:ss)
        #[arg(long)]
        as_of: Option<String>,
    },
}

fn arrow_to_parquet(batch: RecordBatch) -> Result<Vec<u8>, BazofError> {
    let mut buffer = Vec::new();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props))?;

    writer.write(&batch)?;
    writer.close()?;
    Ok(buffer)
}

async fn generate_parquet_test_files(
    path: PathBuf,
    table_name: String,
    file: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let absolute_path = path.canonicalize()?;
    let store_path = Path::from(absolute_path.to_string_lossy().to_string());

    println!("Using lakehouse path: {}", absolute_path.display());

    let local_store = Arc::new(LocalFileSystem::new());
    let schema = Lakehouse::new(store_path, local_store)
        .get_schema(&table_name)
        .await?;

    let table_path = absolute_path.join(table_name);
    let csv = fs::read_to_string(table_path.join(format!("{file}.csv")))?;
    let batch = csv_to_arrow(csv, schema)?;

    let parquet_path = Path::from(table_path.join(format!("{file}.parquet")).to_str().unwrap());

    let local_store = Arc::new(LocalFileSystem::new());
    local_store
        .put(&parquet_path, arrow_to_parquet(batch)?.into())
        .await?;

    println!("Generated test parquet file successfully");
    Ok(())
}

async fn scan_table(
    path: PathBuf,
    table_name: String,
    as_of_str: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let as_of = if let Some(timestamp_str) = as_of_str {
        let timestamp = DateTime::parse_from_rfc3339(&format!("{}.000Z", timestamp_str))
            .map_err(|e| {
                Box::<dyn std::error::Error>::from(format!(
                    "Invalid timestamp format. Expected YYYY-MM-DDTHH:mm:ss, got: {}. Error: {}",
                    timestamp_str, e
                ))
            })?
            .with_timezone(&Utc);

        EventTime(timestamp)
    } else {
        Current
    };

    let absolute_path = path.canonicalize()?;
    let store_path = Path::from(absolute_path.to_string_lossy().to_string());

    println!("Using lakehouse path: {}", absolute_path.display());

    let local_store = Arc::new(LocalFileSystem::new());
    let lakehouse = Lakehouse::new(store_path, local_store);

    let result = lakehouse.scan(&table_name, as_of, All).await?;

    match as_of {
        Current => println!("Scanning table {} (current version):", table_name),
        EventTime(ts) => println!("Scanning table {} as of {}:", table_name, ts),
    }

    print_batch(&result);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Gen { path, table, file } => {
            generate_parquet_test_files(path.clone(), table.clone(), file.clone()).await?;
        }
        Commands::Scan { path, table, as_of } => {
            scan_table(path.clone(), table.clone(), as_of.clone()).await?;
        }
    }

    Ok(())
}
