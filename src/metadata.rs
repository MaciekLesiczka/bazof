use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::errors::BazofError;

#[derive(Serialize, Deserialize, Debug)]
pub struct Snapshot {
    segments: Vec<Segment>,
}

impl Snapshot {
    pub fn deserialize(json_string: &str) -> Result<Snapshot, BazofError>{
        Ok(serde_json::from_str::<Snapshot>(json_string)?)
    }

    pub fn get_data_files(&self, as_of: Option<DateTime<Utc>>) -> Vec<String> {
        self.segments
            .iter()
            .filter(|segment| segment.is_in_range(as_of)) // Only include segments in range
            .filter_map(|segment| segment.file.clone()) // Collect only non-None file values
            .collect()
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Segment {
    id: String,
    #[serde(with = "timestamp_format")]
    start: DateTime<Utc>,
    #[serde(with = "optional_timestamp_format", skip_serializing_if = "Option::is_none", default)]
    end: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    segments: Option<Vec<Segment>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<Vec<Delta>>,
}

impl Segment{
    pub fn is_in_range(&self, as_of: Option<DateTime<Utc>>) -> bool {
        match as_of {
            None => self.end.is_none(),
            Some(as_of_time) => {
                if let Some(end_time) = self.end {
                    self.start <= as_of_time && as_of_time <= end_time
                } else {
                    self.start <= as_of_time
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Delta {
    file: String,
    #[serde(with = "timestamp_format")]
    start: DateTime<Utc>,
    #[serde(with = "timestamp_format")]
    end: DateTime<Utc>,
}

mod timestamp_format {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub const FORMAT: &str = "%Y-%m-%dT%H:%M:%S%.3fZ";

    pub fn serialize<S>(datetime: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = datetime.format(FORMAT).to_string();
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }
}

mod optional_timestamp_format {
    use chrono::{DateTime, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};
    use super::timestamp_format::{FORMAT};

    pub fn serialize<S>(datetime: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match datetime {
            Some(dt) => {
                let s = dt.format(FORMAT).to_string();
                serializer.serialize_some(&s)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Option<String> = Option::deserialize(deserializer)?;
        match s {
            Some(date_str) => {
                DateTime::parse_from_rfc3339(&date_str)
                    .map(|dt| Some(dt.with_timezone(&Utc)))
                    .map_err(serde::de::Error::custom)
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use chrono::{TimeDelta, TimeZone};
    use super::*;
    #[test]
    fn test_deserialization() {
        let json_str =r#"{
  "segments": [
    {
      "id": "10",
      "start": "2024-01-01T00:00:00.000Z",
      "end": "2024-12-31T23:59:59.999Z",
      "file":"base10.parquet",
      "segments": [
        {
          "id": "11",
          "start": "2024-01-01T00:00:00.000Z",
          "end": "2024-06-30T23:59:59.999Z",
          "delta": [
            {
              "file": "delta_111.parquet",
              "start": "2024-01-01T00:00:00.000Z",
              "end": "2024-03-31T23:59:59.999Z"
            },
            {
              "file": "delta_112.parquet",
              "start": "2024-04-01T00:00:00.000Z",
              "end": "2024-06-30T23:59:59.999Z"
            }
          ]
        },
        {
          "id": "12",
          "start": "2024-07-01T00:00:00.000Z",
          "end": "2024-12-31T23:59:59.999Z",
          "file":"base12.parquet",
          "delta": [
            {
              "file": "delta_121.parquet",
              "start": "2024-07-01T00:00:00.000Z",
              "end": "2024-08-31T23:59:59.999Z"
            },
            {
              "file": "delta_122.parquet",
              "start": "2024-09-01T00:00:00.000Z",
              "end": "2024-10-31T23:59:59.999Z"
            },
            {
              "file": "delta_123.parquet",
              "start": "2024-11-01T00:00:00.000Z",
              "end": "2024-12-31T23:59:59.999Z"
            }
          ]
        }
      ]
    },
    {
      "id": "20",
      "start": "2025-01-01T00:00:00.000Z",
      "file":"base20.parquet",
      "delta": [
        {
          "file": "delta_22.parquet",
          "start": "2025-01-01T00:00:00.000Z",
          "end": "2025-01-31T23:59:59.999Z"
        },
        {
          "file": "delta_23.parquet",
          "start": "2025-02-01T00:00:00.000Z",
          "end": "2025-02-11T00:00:00.000Z"
        }
      ]
    }
  ]
}"#;

        let snapshot: Snapshot = serde_json::from_str(json_str).unwrap();

        assert_eq!(snapshot.segments.len(), 2);

        assert_eq!(snapshot.segments[0].id, "10".to_string());
        assert_eq!(snapshot.segments[0].segments.as_ref().unwrap().len(), 2);

        let segment_11 = &snapshot.segments[0].segments.as_ref().unwrap()[0];
        assert_eq!(segment_11.id, "11".to_string());

        assert_eq!(segment_11.start, start_of_month(2024,1));
        assert_eq!(segment_11.file, None);
        assert_eq!(segment_11.end, Some(start_of_month(2024,7).add(TimeDelta::milliseconds(-1))));

        let deltas = segment_11.delta.as_ref().unwrap();
        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].file, "delta_111.parquet");
        assert_eq!(deltas[1].end, start_of_month(2024,7).add(TimeDelta::milliseconds(-1)));

        assert_eq!(snapshot.segments[1].end, None);
    }

    #[test]
    fn test_serialization() {
        let snapshot = Snapshot {
            segments: vec![
                Segment {
                    id: "10".to_string(),
                    start: start_of_month(2025, 1),
                    end: Some(start_of_month(2025, 2).add(TimeDelta::milliseconds(-1))),
                    file: Some("base.parquet".to_string()),
                    segments: Some(vec![
                        Segment {
                            id: "11".to_string(),
                            start: start_of_month(2025, 1),
                            end: Some(start_of_month(2025, 2).add(TimeDelta::milliseconds(-1))),
                            file: Some("base.parquet".to_string()),
                            segments: None,
                            delta: Some (
                                vec![Delta{
                                    start: start_of_month(2025, 1),
                                    end: start_of_month(2025, 2).add(TimeDelta::milliseconds(-1)),
                                    file: "delta_111.parquet".to_string(),
                                }
                            ])
                        }
                    ]),
                    delta: None,
                }
            ],
        };

        let json_str = serde_json::to_string(&snapshot).unwrap();
        let deserialized_snapshot: Snapshot = serde_json::from_str(&json_str).unwrap();
        assert_eq!(deserialized_snapshot.segments.len(), 1);
        assert_eq!(deserialized_snapshot.segments[0].segments.as_ref().unwrap().len(), 1);
        let nested_segment = &deserialized_snapshot.segments[0].segments.as_ref().unwrap()[0];

        assert_eq!(nested_segment.id, "11".to_string());
        assert_eq!(nested_segment.file.as_ref().unwrap(), "base.parquet");
        assert_eq!(nested_segment.end.unwrap(), start_of_month(2025, 2).add(TimeDelta::milliseconds(-1)));

        let delta = &nested_segment.delta.as_ref().unwrap()[0];
        assert_eq!(delta.start, start_of_month(2025, 1));
        assert_eq!(delta.end, start_of_month(2025, 2).add(TimeDelta::milliseconds(-1)));
        assert_eq!(delta.file, "delta_111.parquet".to_string());
    }

    #[test]
    fn reads_base_file_of_current_segment(){
        let json_str = r#"{
  "segments": [
    {
      "id": "10",
      "start": "2024-01-01T00:00:00.000Z",
      "file": "base.parquet"
    }
  ]
}"#;
        let snapshot = Snapshot::deserialize(json_str).unwrap();

        let files = snapshot.get_data_files(None);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], String::from("base.parquet"));

        let files = snapshot.get_data_files(Some(start_of_month(2023,12)));

        assert_eq!(files.len(), 0);
    }

    #[test]
    fn reads_base_file_of_historical_segment(){
        let json_str = r#"{
  "segments": [
    {
      "id": "10",
      "start": "2024-01-01T00:00:00.000Z",
      "end": "2024-03-01T00:00:00.000Z",
      "file": "base.parquet"
    }
  ]
}"#;
        let snapshot = Snapshot::deserialize(json_str).unwrap();

        let files = snapshot.get_data_files(Some(start_of_month(2024,1)));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], String::from("base.parquet"));

        let files = snapshot.get_data_files(Some(start_of_month(2024,2)));
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], String::from("base.parquet"));

        let files = snapshot.get_data_files(None);

        assert_eq!(files.len(), 0);

        let files = snapshot.get_data_files(Some(start_of_month(2023,2)));
        assert_eq!(files.len(), 0);

        let files = snapshot.get_data_files(Some(start_of_month(2024,4)));
        assert_eq!(files.len(), 0);
    }

    fn start_of_month(year:i32, month:u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, 1,0, 0, 0).unwrap()
    }
}