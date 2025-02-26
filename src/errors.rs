use arrow::error::ArrowError;
use chrono::ParseError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BazofError {
    #[error("IO error: {0}")]
    Arrow(#[from] ArrowError),
    #[error("Parsing error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("DateTime parsing error: {0}")]
    ParseChrono(#[from] ParseError),
    #[error("Parquet file error: {0}")]
    ParquetFile(#[from] parquet::errors::ParquetError),
}