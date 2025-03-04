use std::sync::Arc;
use chrono::{TimeZone, Utc};
use object_store::{path::Path, ObjectStore, PutPayload};
use object_store::memory::InMemory;
use crate::as_of::AsOf;
use crate::as_of::AsOf::{Current, Past};
use crate::errors::BazofError;
use crate::metadata::Snapshot;

pub struct Table {
    path: Path,
    store: Arc<dyn ObjectStore>
}

impl Table {
    pub fn new(absolute_path: &str, store: Arc<dyn ObjectStore>) -> Self {
        Table {
            path: Path::from(absolute_path),
            store
        }
    }

    pub async fn get_data_files(&self, as_of: AsOf) -> Result<Vec<String>, BazofError> {
        let snapshot = self.read_snapshot("s.json").await?;
        let files = snapshot.get_data_files(as_of);
        Ok(files)
    }

    async fn read_snapshot(&self, file_name:&str) -> Result<Snapshot, BazofError> {
        let snapshot_path = self.path.child(file_name);
        let file = self.store.get(&snapshot_path).await?;
        let reader = file.bytes().await?;
        let result = String::from_utf8(reader.to_vec())?; // Convert to String

        Ok(Snapshot::deserialize(&result)?)
    }
}


#[tokio::test]
async fn read_files_from_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let table = Table::new("bazof/table0", store.clone());

    let snapshot_data = r#"{
        "segments": [
            {
                "id": "10",
                "start": "2024-01-01T00:00:00.000Z",
                "file": "base10.parquet"
            }
        ]
    }"#;

    let snapshot_path = Path::from("bazof/table0").child("s.json");

    let put_payload = PutPayload::from(snapshot_data.as_bytes());
    store.put(&snapshot_path,put_payload ).await?;

    let files = table.get_data_files(Current).await?;
    assert_eq!(files, vec!["base10.parquet".to_string()]);

    let past = Utc.with_ymd_and_hms(2020, 1, 17,0, 0, 0).unwrap();

    let files = table.get_data_files(Past(past)).await?;
    assert_eq!(files.len(),0);

    Ok(())
}
