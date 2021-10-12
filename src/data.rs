use std::convert::TryInto;

use serde::{Deserialize, Serialize};

// I64, Date, Datetime, Time, Json

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Type {
    U64,
    String,
    OptionU64,
    Lancer,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Data {
    U64(u64),
    String(String),
    OptionU64(Option<u64>),
    Lancer(u16),
}

impl Type {
    pub fn size(&self) -> Option<usize> {
        match self {
            Type::U64 => Some(8),
            Type::String => None,
            Type::OptionU64 => Some(9),
            Type::Lancer => None,
        }
    }
}

impl Data {
    pub fn size(&self) -> usize {
        match self {
            Data::U64(_) => 8,
            Data::String(s) => s.as_bytes().len(),
            Data::OptionU64(_) => 9,
            Data::Lancer(size) => *size as usize,
        }
    }
}

impl PartialOrd for Data {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // match (self, other) {
        //     (Data::U64(left), Data::U64(right)) => {
        //         left.to_le_bytes().partial_cmp(&right.to_le_bytes())
        //     },
        //     (Data::String(left), Data::String(right)) => todo!(),
        //     (Data::OptionU64(left), Data::OptionU64(right)) => todo!(),
        //     (Data::Lancer(left), Data::Lancer(right)) => todo!(),
        //     _ => panic!("different types are compared")
        // }

        // TODO: refine!
        data_vec_to_bytes(&[self.clone()]).partial_cmp(&data_vec_to_bytes(&[other.clone()]))
    }
}

impl std::fmt::Display for Data {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Data::U64(v) => write!(f, "{:?}", v),
            Data::String(v) => write!(f, "{:?}", v),
            Data::OptionU64(v) => {
                if let Some(v) = v {
                    write!(f, "{:?}", v)
                } else {
                    write!(f, "null")
                }
            }
            Data::Lancer(size) => write!(f, "<{}>", size),
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
            Type::OptionU64 => {
                if bytes[i] == 0 {
                    vec.push(Data::OptionU64(None));
                } else {
                    vec.push(Data::OptionU64(Some(parse_u64(&bytes[i + 1..i + 1 + 8]))));
                }
                i += 9;
            }
            Type::Lancer => {
                let size = parse_u16(&bytes[i..i + 2]) as usize;
                vec.push(Data::Lancer(size as u16));
                i += 2 + size;
            }
        }
    }
    debug_assert_eq!(bytes.len(), i, "length mismatched, bytes: {:?}", &bytes);
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
            Data::OptionU64(v) => {
                if let Some(v) = v {
                    bytes.push(1);
                    bytes.extend(v.to_le_bytes());
                } else {
                    bytes.extend([0; 9]);
                }
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
