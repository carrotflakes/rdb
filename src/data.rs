#[derive(Debug, Clone)]
pub enum Type {
    U64,
    String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Data {
    U64(u64),
    String(String),
}
