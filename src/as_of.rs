use chrono::{DateTime, Utc};

#[derive(Copy, Eq, Debug, Hash, PartialEq)]
#[derive(Clone)]
pub enum AsOf {
    Current,
    Past(DateTime<Utc>),
}
