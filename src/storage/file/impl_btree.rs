use std::convert::TryInto;

use crate::btree::{BTree, BTreeNode};

use super::{pager::PAGE_SIZE, File, Page};

const HEADER_SIZE: usize = 1 + 4 + 2 + 4;
const INDEX_SIZE: usize = 2;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

pub type BTreeCursor = crate::btree::BTreeCursor<Key, Value, File>;

// # layout
// [1] leaf flag // TODO なくしたい
// [4] parent node id
// [2] size
// [4] next node id
// ...
//
// ## key_size 0, value_size fixed
// value...
//
// ## key_size 0, value_size variable
// value_index... ...value
//
// ## key_size fixed, value_size fixed
// key..., value...
//
// ## key_size fixed, value_size variable
// (key, value_index)..., ...value
//
// ## key_size variable, value_size fixed
// (key_size, key)..., ...value
//
// ## key_size variable, value_size variable
// (key_size, key, value_index)..., ...value

#[derive(Debug)]
pub struct Meta {
    pub key_size: Option<usize>,
    pub value_size: Option<usize>,
}

impl BTreeNode<Key, Value> for Page {
    type Meta = Meta;
    type Cursor = usize; // = key index

    fn is_leaf(&self, _: &Self::Meta) -> bool {
        self[0] == 1
    }

    fn get_parent(&self, _: &Self::Meta) -> Option<usize> {
        let i = parse_u32(&self[1..1 + 4]);
        if i == u32::MAX {
            None
        } else {
            Some(i as usize)
        }
    }

    fn set_parent(&mut self, _: &Self::Meta, i: usize) {
        self[1..1 + 4].copy_from_slice(&u32::to_le_bytes(i as u32))
    }

    fn size(&self, _: &Self::Meta) -> usize {
        parse_u16(&self[1 + 4..1 + 4 + 2]) as usize
    }

    fn is_full(&self, meta: &Self::Meta) -> bool {
        debug_assert!(!self.is_leaf(meta));
        if let Some(key_size) = meta.key_size {
            // fixed key_size:
            let value_size = 4;
            let size = self.size(meta);
            let size_max = (PAGE_SIZE - (1 + 4 + 2 + 4)) / (key_size + value_size) as u64;
            size as u64 == size_max
        } else {
            // variable key_size:
            panic!("variable key_size is not supported")
        }
    }

    fn insert(&mut self, meta: &Self::Meta, key: &Key, value: &Value) -> bool {
        let size = self.size(meta);
        let res = match meta {
            Meta {
                key_size: Some(0),
                value_size: Some(value_size),
            } => todo!(),
            Meta {
                key_size: Some(0),
                value_size: None,
            } => todo!(),
            Meta {
                key_size: Some(key_size),
                value_size: Some(value_size),
            } => {
                assert_eq!(*key_size, key.len());
                assert_eq!(*value_size, value.len());

                // check if insertable
                let insert_data_size = *key_size + *value_size;
                let remain_data_size =
                    PAGE_SIZE as usize - (HEADER_SIZE + (key_size + value_size) * size);
                if insert_data_size > remain_data_size {
                    return false;
                }

                // find insert index
                let mut insert_index = size;
                for i in 0..size {
                    let offset = HEADER_SIZE + key_size * i;
                    let k = &self[offset..offset + key_size];
                    if key.as_slice() < k {
                        insert_index = i;
                        break;
                    }
                }

                let capacity = (PAGE_SIZE as usize - HEADER_SIZE) / insert_data_size;
                let values_offset = HEADER_SIZE + capacity * *key_size;

                // move forward keys and values
                let key_offset = HEADER_SIZE + key_size * insert_index;
                self.copy_within(
                    key_offset..HEADER_SIZE + key_size * size,
                    key_offset + key_size,
                );
                let value_offset = values_offset + value_size * insert_index;
                self.copy_within(
                    value_offset..values_offset + value_size * size,
                    value_offset + value_size,
                );

                // insert
                self[key_offset..key_offset + key_size].copy_from_slice(key);
                self[value_offset..value_offset + value_size].copy_from_slice(value);

                true
            }
            Meta {
                key_size: Some(key_size),
                value_size: None,
            } => {
                assert_eq!(*key_size, key.len());

                let key_interval = key_size + INDEX_SIZE;

                // check if insertable
                let last_value_index = if size == 0 {
                    PAGE_SIZE as usize
                } else {
                    let last_value_index_index = HEADER_SIZE + key_interval * size - INDEX_SIZE;
                    parse_u16(&self[last_value_index_index..last_value_index_index + INDEX_SIZE])
                        as usize
                };
                let insert_data_size = *key_size + value.len();
                let remain_data_size = last_value_index - (HEADER_SIZE + key_interval * size);
                if insert_data_size > remain_data_size {
                    return false;
                }

                // find insert index
                let mut insert_index = size;
                for i in 0..size {
                    let offset = HEADER_SIZE + key_interval * i;
                    let k = &self[offset..offset + key_size];
                    if key.as_slice() < k {
                        insert_index = i;
                        break;
                    }
                }

                // move forward keys and values
                let key_offset = HEADER_SIZE + key_interval * insert_index;
                self.copy_within(
                    key_offset..HEADER_SIZE + key_interval * size,
                    key_offset + key_interval,
                );
                let end = if insert_index < size {
                    parse_u16(&self[key_offset - INDEX_SIZE..key_offset])
                        as usize
                } else {
                    last_value_index
                };
                self.copy_within(last_value_index..end, last_value_index - value.len());

                // recalculate value_index
                for i in (insert_index..size).rev() {
                    let offset = HEADER_SIZE + key_interval * (i + 1) + key_size;
                    let s = parse_u16(&self[offset..offset + INDEX_SIZE]);
                    self[offset..offset + INDEX_SIZE]
                        .copy_from_slice(&(s - value.len() as u16).to_le_bytes());
                }
                let value_index_offset = HEADER_SIZE + key_interval * insert_index + key_size;
                let s = if insert_index == 0 {
                    PAGE_SIZE as u16
                } else {
                    parse_u16(&self[value_index_offset - key_interval..value_index_offset - key_interval + INDEX_SIZE])
                };
                self[value_index_offset..value_index_offset + INDEX_SIZE]
                    .copy_from_slice(&(s - value.len() as u16).to_le_bytes());

                // insert
                self[key_offset..key_offset + key_size].copy_from_slice(key);
                self[end - value.len()..end].copy_from_slice(value);

                true
            }
            Meta {
                key_size: None,
                value_size: Some(value_size),
            } => todo!(),
            Meta {
                key_size: None,
                value_size: None,
            } => todo!(),
        };
        if res {
            // increment size
            let size = parse_u16(&self[1 + 4..1 + 4 + 2]) + 1;
            self[1 + 4..1 + 4 + 2].copy_from_slice(&u16::to_le_bytes(size));
        }
        res
    }

    fn insert_node(&mut self, meta: &Self::Meta, key: &Key, node_i: usize) {
        todo!()
    }

    fn get(&self, meta: &Self::Meta, key: &Key) -> Option<Value> {
        todo!()
    }

    fn get_child(&self, meta: &Self::Meta, key: &Key) -> usize {
        let size = self.size(meta);
        if size == 0 {
            return 0;
        }
        match &meta.key_size {
            Some(0) => 0,
            Some(key_size) => {
                for i in 0..size - 1 {
                    let offset = HEADER_SIZE + key_size * i;
                    let k = &self[offset..offset + key_size];
                    if key.as_slice() < k {
                        let offset = PAGE_SIZE as usize - INDEX_SIZE * (i - 1);
                        return parse_u16(&self[offset..offset + INDEX_SIZE]) as usize;
                    }
                }
                let offset = PAGE_SIZE as usize - INDEX_SIZE * size;
                parse_u16(&self[offset..offset + INDEX_SIZE]) as usize
            }
            None => todo!(),
        }
    }

    fn get_first_child(&self, meta: &Self::Meta) -> usize {
        todo!()
    }

    fn get_children(&self, meta: &Self::Meta) -> Vec<usize> {
        let size = self.size(meta);
        match meta {
            Meta {
                key_size: Some(0),
                value_size: Some(value_size),
            } => todo!(),
            Meta {
                key_size: Some(0),
                value_size: None,
            } => todo!(),
            Meta {
                key_size: Some(key_size),
                value_size: Some(value_size),
            } => todo!(),
            Meta {
                key_size: Some(key_size),
                value_size: None,
            } => {
                // for i in 0..size {
                //     let offset = HEADER_SIZE + (key_size + INDEX_SIZE) * i;
                //     let k = bincode::deserialize(&self[offset..offset+key_size]).unwrap();
                // }
                todo!()
            }
            Meta {
                key_size: None,
                value_size: Some(value_size),
            } => todo!(),
            Meta {
                key_size: None,
                value_size: None,
            } => todo!(),
        }
    }

    fn remove(&mut self, meta: &Self::Meta, key: &Key) -> bool {
        todo!()
    }

    fn split_out(&mut self, meta: &Self::Meta) -> (Key, Self) {
        todo!()
    }

    fn new_internal(meta: &Self::Meta) -> Self {
        [0; PAGE_SIZE as usize].into()
    }

    fn init_as_root_internal(&mut self, meta: &Self::Meta, key: &Key, i1: usize, i2: usize) {
        todo!()
    }

    fn first_cursor(&self, meta: &Self::Meta) -> Self::Cursor {
        0
    }

    fn find(&self, meta: &Self::Meta, key: &Key) -> Option<Self::Cursor> {
        let size = self.size(meta);
        match meta {
            Meta {
                key_size: Some(0),
                value_size: Some(value_size),
            } => todo!(),
            Meta {
                key_size: Some(0),
                value_size: None,
            } => todo!(),
            Meta {
                key_size: Some(key_size),
                value_size: Some(_),
            } => {
                for i in 0..size {
                    let offset = HEADER_SIZE + key_size * i;
                    let k = &self[offset..offset + key_size];
                    if key.as_slice() <= k {
                        return Some(i);
                    }
                }
                Some(size)
            }
            Meta {
                key_size: Some(key_size),
                value_size: None,
            } => {
                for i in 0..size {
                    let offset = HEADER_SIZE + (key_size + INDEX_SIZE) * i;
                    let k = &self[offset..offset + key_size];
                    if key.as_slice() <= k {
                        return Some(i);
                    }
                }
                Some(size)
            }
            Meta {
                key_size: None,
                value_size: Some(value_size),
            } => todo!(),
            Meta {
                key_size: None,
                value_size: None,
            } => todo!(),
        }
    }

    fn cursor_get(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> Option<(Key, Value)> {
        let size = self.size(meta);
        match meta {
            Meta {
                key_size: Some(0),
                value_size: Some(value_size),
            } => todo!(),
            Meta {
                key_size: Some(0),
                value_size: None,
            } => todo!(),
            Meta {
                key_size: Some(key_size),
                value_size: Some(_),
            } => {
                todo!();
            }
            Meta {
                key_size: Some(key_size),
                value_size: None,
            } => {
                let key_interval = key_size + INDEX_SIZE;
                let value_index_index = HEADER_SIZE + key_interval * cursor + key_size;
                let value_start =
                    parse_u16(&self[value_index_index..value_index_index + INDEX_SIZE]) as usize;
                let value_end = if *cursor == 0 {
                    PAGE_SIZE as usize
                } else {
                    let value_index_index = HEADER_SIZE + key_interval * (cursor - 1) + key_size;
                    parse_u16(&self[value_index_index..value_index_index + INDEX_SIZE]) as usize
                };
                let key_index = HEADER_SIZE + key_interval * cursor;
                let key = self[key_index..key_index + key_size].to_vec();
                let value = self[value_start..value_end].to_vec();

                Some((key, value))
            }
            Meta {
                key_size: None,
                value_size: Some(value_size),
            } => todo!(),
            Meta {
                key_size: None,
                value_size: None,
            } => todo!(),
        }
    }

    fn cursor_next(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> (usize, Self::Cursor) {
        if self.size(meta) <= cursor + 1 {
            (parse_u32(&self[1 + 4 + 2..1 + 4 + 2 + 4]) as usize, 0)
        } else {
            (usize::MAX, cursor + 1)
        }
    }

    fn cursor_is_end(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> bool {
        self.size(meta) <= *cursor && {
            let next = parse_u32(&self[1 + 4 + 2..1 + 4 + 2 + 4]);
            next == 0
        }
    }
}

impl BTree<Key, Value> for File {
    type Node = Page;

    fn add_root_node(&mut self) -> usize {
        let page_i = self.pager.size();
        let page = self.pager.get_mut(page_i);

        page[0] = 1;
        page[1..1 + 4].copy_from_slice(&u32::to_le_bytes(0 as u32));
        page[1 + 4..1 + 4 + 2].copy_from_slice(&u16::to_le_bytes(0 as u16));
        page[1 + 4 + 2..1 + 4 + 2 + 4].copy_from_slice(&u32::to_le_bytes(0 as u32));

        page_i
    }

    fn node_ref(&self, node_i: usize) -> &Self::Node {
        #[allow(mutable_transmutes)]
        unsafe { std::mem::transmute::<_, &mut super::pager::Pager<Page>>(&self.pager) }
            .get_ref(node_i)
    }

    fn node_mut(&mut self, node_i: usize) -> &mut Self::Node {
        self.pager.get_mut(node_i)
    }

    fn push(&mut self, node: Self::Node) -> usize {
        self.pager.push(node)
    }

    fn swap(&mut self, node_i: usize, node: Self::Node) -> Self::Node {
        self.pager.swap(node_i, node)
    }
}

pub fn parse_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().unwrap())
}

pub fn parse_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().unwrap())
}
