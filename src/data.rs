use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Type {
    U64,
    String,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Data {
    U64(u64),
    String(String),
}

impl Type {
    pub fn size(&self) -> Option<usize> {
        match self {
            Type::U64 => Some(8),
            Type::String => None,
        }
    }
}

impl Data {
    pub fn size(&self) -> usize {
        match self {
            Data::U64(_) => 8,
            Data::String(s) => s.as_bytes().len(),
        }
    }

    pub fn from_bytes(ty: Type, bytes: &[u8]) -> Option<Self> {
        match ty {
            Type::U64 => todo!(),
            Type::String => String::from_utf8(bytes.to_vec()).map(Data::String).ok(),
        }
    }
}
