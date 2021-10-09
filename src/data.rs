use std::convert::TryInto;

use serde::{Deserialize, Serialize};

// I64, Date, Datetime, Time, Json

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Type {
    U64,
    String,
    Lancer,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Data {
    U64(u64),
    String(String),
    Lancer(u16),
}

impl Type {
    pub fn size(&self) -> Option<usize> {
        match self {
            Type::U64 => Some(8),
            Type::String => None,
            Type::Lancer => None,
        }
    }
}

impl Data {
    pub fn size(&self) -> usize {
        match self {
            Data::U64(_) => 8,
            Data::String(s) => s.as_bytes().len(),
            Data::Lancer(size) => *size as usize,
        }
    }
}

pub fn data_vec_from_bytes(types: &[Type], bytes: &[u8]) -> Option<Vec<Data>> {
    let mut i = 0;
    let mut vec = Vec::with_capacity(types.len());
    // dbg!((&types, &bytes));
    for typ in types {
        match typ {
            Type::U64 => {
                vec.push(Data::U64(parse_u64(&bytes[i..i + 8])));
                i += 8;
            }
            Type::String => {
                let size = parse_u16(&bytes[i..i + 2]) as usize;
                vec.push(Data::String(
                    String::from_utf8(bytes[i + 2..i + 2 + size].to_vec())
                        .ok()
                        .unwrap(),
                ));
                i += 2 + size;
            }
            Type::Lancer => {
                let size = parse_u16(&bytes[i..i + 2]) as usize;
                vec.push(Data::Lancer(size as u16));
                i += 2 + size;
            }
        }
    }
    debug_assert_eq!(bytes.len(), i);
    Some(vec)
}

pub fn data_vec_to_bytes(datas: &[Data]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for data in datas {
        match data {
            Data::U64(v) => bytes.extend(v.to_le_bytes()),
            Data::String(s) => {
                bytes.extend((s.len() as u16).to_le_bytes());
                bytes.extend(s.as_bytes())
            }
            Data::Lancer(size) => {
                bytes.extend(size.to_le_bytes());
                bytes.extend((0..*size).map(|_| 0));
            }
        }
    }
    // dbg!((&datas, &bytes));
    bytes
}

fn parse_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().unwrap())
}

fn parse_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().unwrap())
}

#[test]
fn test() {
    let datas = vec![
        Data::U64(123),
        Data::String("hello, わーるど😸".to_owned()),
        Data::Lancer(10),
        Data::U64(321),
    ];
    let bytes = data_vec_to_bytes(&datas);
    dbg!(&bytes);
    let decoded = data_vec_from_bytes(
        &vec![Type::U64, Type::String, Type::Lancer, Type::U64],
        &bytes,
    )
    .unwrap();
    assert_eq!(datas, decoded);
}
