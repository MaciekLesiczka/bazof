extern crate core;

mod test_bench;

use bazof::AsOf::{Current, EventTime};
use bazof::Lakehouse;
use bazof::BazofError;
use test_bench::{csv_to_arrow, print_batch};
use arrow_array::RecordBatch;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use object_store::local::LocalFileSystem;
use object_store::{path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate test parquet files from CSV files
    Gen,
    
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

async fn generate_parquet_test_files() -> Result<(), Box<dyn std::error::Error>> {
    let mut workspace_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    workspace_dir.pop();
    workspace_dir.pop();
    
    let test_data_path = workspace_dir.join("test-data");
    let curr_dir = Path::from(test_data_path.to_str().unwrap());
    
    let local_store = Arc::new(LocalFileSystem::new());

    let base_csv_path = workspace_dir.join("test-data").join("table0").join("base.csv");
    let csv = fs::read_to_string(base_csv_path)?;
    let batch = csv_to_arrow(csv)?;
    local_store.put(&curr_dir.child("table0").child("base.parquet"),
                    arrow_to_parquet(batch)?.into()).await?;

    let delta_csv_path = workspace_dir.join("test-data").join("table0").join("delta1.csv");
    let csv = fs::read_to_string(delta_csv_path)?;
    let batch = csv_to_arrow(csv)?;
    local_store.put(&curr_dir.child("table0").child("delta1.parquet"),
                    arrow_to_parquet(batch)?.into()).await?;
    
    println!("Generated test parquet files successfully");
    Ok(())
}

async fn scan_table(path: PathBuf, table_name: String, as_of_str: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let as_of = if let Some(timestamp_str) = as_of_str {
        let timestamp = DateTime::parse_from_rfc3339(&format!("{}.000Z", timestamp_str))
            .map_err(|e| Box::<dyn std::error::Error>::from(
                format!("Invalid timestamp format. Expected YYYY-MM-DDTHH:mm:ss, got: {}. Error: {}", 
                        timestamp_str, e)
            ))?
            .with_timezone(&Utc);
        
        EventTime(timestamp)
    } else {
        Current
    };
    
    let absolute_path = path.canonicalize()
        .map_err(|e| Box::<dyn std::error::Error>::from(
            format!("Error resolving path {}: {}", path.display(), e)
        ))?;
    
    let store_path = Path::from(absolute_path.to_string_lossy().to_string());
    
    println!("Using lakehouse path: {}", absolute_path.display());
    
    let local_store = Arc::new(LocalFileSystem::new());
    let lakehouse = Lakehouse::new(store_path, local_store);

    let result = lakehouse.scan(&table_name, as_of).await?;
    
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
        Commands::Gen => {
            generate_parquet_test_files().await?;
        },
        Commands::Scan { path, table, as_of } => {
            scan_table(path.clone(), table.clone(), as_of.clone()).await?;
        }
    }
    
    Ok(())
}