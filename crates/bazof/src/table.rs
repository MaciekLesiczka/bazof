use crate::errors::BazofError;
use crate::metadata::Snapshot;
use object_store::{path::Path, ObjectStore};
use std::sync::Arc;

pub struct Table {
    pub path: Path,
    store: Arc<dyn ObjectStore>,
}

impl Table {
    pub fn new(path: Path, store: Arc<dyn ObjectStore>) -> Self {
        Table { path, store }
    }

    pub async fn get_current_snapshot(&self) -> Result<Snapshot, BazofError> {
        let snapshot_id = self.read_version().await?;
        Ok(self.get_snapshot(&snapshot_id).await?)
    }

    pub async fn get_snapshot(&self, snapshot_id: &str) -> Result<Snapshot, BazofError> {
        let snapshot_file = format!("s{}.json", snapshot_id);
        Ok(self.read_snapshot(&snapshot_file).await?)
    }

    async fn read_snapshot(&self, file_name: &str) -> Result<Snapshot, BazofError> {
        let snapshot_path = self.path.child(file_name);
        let file = self.store.get(&snapshot_path).await?;
        let reader = file.bytes().await?;
        let result = String::from_utf8(reader.to_vec())?;

        Snapshot::deserialize(&result)
    }

    async fn read_version(&self) -> Result<String, BazofError> {
        let snapshot_path = self.path.child("version.txt");
        let file = self.store.get(&snapshot_path).await?;
        let reader = file.bytes().await?;
        let result = String::from_utf8(reader.to_vec())?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::as_of::AsOf::{Current, EventTime};
    use crate::table::Table;
    use chrono::{TimeZone, Utc};
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, PutPayload};
    use std::sync::Arc;

    #[tokio::test]
    async fn read_files_from_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(InMemory::new());
        let table = Table::new(Path::from("bazof/table0"), store.clone());

        put_table_metadata(
            &store,
            "s1.json",
            String::from(
                r#"{
        "schema": {
          "columns": [
            {
              "name": "value",
              "data_type": "String",
              "nullable": false
            }
          ]
        },
        "segments": [
            {
                "id": "10",
                "start": "2024-01-01T00:00:00.000Z",
                "file": "base10.parquet"
            }
        ]
    }"#,
            ),
        )
        .await;

        put_table_metadata(
            &store,
            "s2.json",
            String::from(
                r#"{
        "schema": {
          "columns": [
            {
              "name": "value",
              "data_type": "String",
              "nullable": false
            }
          ]
        },
        "segments": [
            {
                "id": "10",
                "start": "2024-01-01T00:00:00.000Z",
                "file": "base101.parquet"
            }
        ]
    }"#,
            ),
        )
        .await;

        put_table_metadata(&store, "version.txt", String::from("1")).await;

        let files = table
            .get_snapshot("1")
            .await
            .unwrap()
            .get_data_files(Current);
        assert_eq!(files, vec!["base10.parquet".to_string()]);

        let past = Utc.with_ymd_and_hms(2020, 1, 17, 0, 0, 0).unwrap();
        let files = table
            .get_snapshot("1")
            .await
            .unwrap()
            .get_data_files(EventTime(past));
        assert_eq!(files.len(), 0);

        let files = table
            .get_snapshot("2")
            .await
            .unwrap()
            .get_data_files(Current);
        assert_eq!(files, vec!["base101.parquet".to_string()]);

        let files = table
            .get_current_snapshot()
            .await
            .unwrap()
            .get_data_files(Current);
        assert_eq!(files, vec!["base10.parquet".to_string()]);

        Ok(())
    }

    async fn put_table_metadata(store: &Arc<InMemory>, file_name: &str, data: String) {
        let snapshot_path = Path::from("bazof/table0").child(file_name);
        let put_payload = PutPayload::from(data.as_bytes().to_vec());
        let _ = store.put(&snapshot_path, put_payload).await;
    }
}
