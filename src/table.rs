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

    pub async fn get_data_files(&self, snapshot_id:&str, as_of: AsOf) -> Result<Vec<String>, BazofError> {
        let snapshot_file = format!("s{}.json", snapshot_id);
        let snapshot = self.read_snapshot(snapshot_file.as_str()).await?;
        let files = snapshot.get_data_files(as_of);
        Ok(files)
    }

    pub async fn get_current_data_files(&self, as_of: AsOf) -> Result<Vec<String>, BazofError> {

        let version = self.read_version().await?;
        let snapshot_file = format!("s{}.json", version);
        let snapshot = self.read_snapshot(snapshot_file.as_str()).await?;
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

    async fn read_version(&self) -> Result<String, BazofError> {
        let snapshot_path = self.path.child("version.txt");
        let file = self.store.get(&snapshot_path).await?;
        let reader = file.bytes().await?;
        let result = String::from_utf8(reader.to_vec())?; // Convert to String
        Ok(result)
    }
}


#[tokio::test]
async fn read_files_from_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemory::new());
    let table = Table::new("bazof/table0", store.clone());

    put_table_metadata( &store,"s1.json", String::from(r#"{
        "segments": [
            {
                "id": "10",
                "start": "2024-01-01T00:00:00.000Z",
                "file": "base10.parquet"
            }
        ]
    }"#)).await;

    put_table_metadata( &store,"s2.json", String::from(r#"{
        "segments": [
            {
                "id": "10",
                "start": "2024-01-01T00:00:00.000Z",
                "file": "base101.parquet"
            }
        ]
    }"#)).await;

    put_table_metadata( &store,"version.txt", String::from("1")).await;

    let files = table.get_data_files("1",Current).await?;
    assert_eq!(files, vec!["base10.parquet".to_string()]);

    let past = Utc.with_ymd_and_hms(2020, 1, 17,0, 0, 0).unwrap();
    let files = table.get_data_files("1", Past(past)).await?;
    assert_eq!(files.len(),0);

    let files = table.get_data_files("2",Current).await?;
    assert_eq!(files, vec!["base101.parquet".to_string()]);

    let files = table.get_current_data_files(Current).await?;
    assert_eq!(files, vec!["base10.parquet".to_string()]);

    Ok(())
}


async fn put_table_metadata(store: &Arc<InMemory> , file_name : &str, data:String) {
    let snapshot_path = Path::from("bazof/table0").child(file_name);
    let put_payload = PutPayload::from(data.as_bytes().to_vec());
    let _ = store.put(&snapshot_path,put_payload ).await;
}