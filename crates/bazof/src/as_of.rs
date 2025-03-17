use chrono::{DateTime, Utc};

#[derive(Copy, Eq, Debug, Hash, PartialEq, Clone)]
pub enum AsOf {
    Current,
    EventTime(DateTime<Utc>),
}
