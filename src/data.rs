use std::convert::TryInto;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Type {
    U64,
    String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash, Serialize, Deserialize)]
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
        Data::U64(321),
    ];
    let bytes = data_vec_to_bytes(&datas);
    dbg!(&bytes);
    let decoded = data_vec_from_bytes(&vec![Type::U64, Type::String, Type::U64], &bytes).unwrap();
    assert_eq!(datas, decoded);
}
