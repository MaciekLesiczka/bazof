

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Snapshot {
    segments: Vec<Segment>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Segment {
    id: u64,
    start: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    end: Option<u64>,
    location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    base: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    segments: Option<Vec<Segment>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delta: Option<Vec<Delta>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Delta {
    file: String,
    start: u64,
    end: u64,
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_deserialization() {
        let json_str =r#"{
  "segments": [
    {
      "id": 10,
      "start": 0,
      "end": 999,
      "location": "data/segments/10",
      "base":"base.parquet",
      "segments": [
        {
          "id": 11,
          "start":0,
          "end": 499,
          "location": "data/segments/11",
          "delta": [
            {
              "file": "delta_111.parquet",
              "start": 12,
              "end": 100
            },
            {
              "file": "delta_112.parquet",
              "start": 220,
              "end": 399
            }
          ]
        },
        {
          "id": 12,
          "start":500,
          "end": 999,
          "base":"base.parquet",
          "location": "data/segments/11",
          "delta": [
            {
              "file": "delta_121.parquet",
              "start": 500,
              "end": 699
            },
            {
              "file": "delta_122.parquet",
              "start": 700,
              "end": 899
            },
            {
              "file": "delta_123.parquet",
              "start": 900,
              "end": 999
            }
          ]
        }
      ]
    },
    {
      "id": 20,
      "start": 1000,
      "location": "data/segments/20",
      "base":"base.parquet",
      "delta": [
        {
          "file": "delta_22.parquet",
          "start": 1200,
          "end": 1405
        },
        {
          "file": "delta_23.parquet",
          "start": 1600,
          "end": 1890
        }
      ]
    }
  ]
}"#;

        let snapshot: Snapshot = serde_json::from_str(json_str).unwrap();

        assert_eq!(snapshot.segments.len(), 2);
        assert_eq!(snapshot.segments[0].id, 10);
        assert_eq!(snapshot.segments[0].segments.as_ref().unwrap().len(), 2);

        let segment_11 = &snapshot.segments[0].segments.as_ref().unwrap()[0];
        assert_eq!(segment_11.id, 11);
        assert_eq!(segment_11.delta.as_ref().unwrap().len(), 2);
        assert_eq!(segment_11.delta.as_ref().unwrap()[0].file, "delta_111.parquet");
    }
}