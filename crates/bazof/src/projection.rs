use std::collections::HashSet;

#[derive(Eq, Debug, PartialEq, Clone)]
pub enum Projection {
    All,
    Columns(HashSet<String>),
}

impl Projection {
    pub fn contains(&self, column: &str) -> bool {
        if let Projection::Columns(columns) = self {
            columns.contains(column)
        } else {
            true
        }
    }
}
