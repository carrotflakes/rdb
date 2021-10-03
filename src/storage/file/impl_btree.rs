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
            } => {
                assert_eq!(*key_size, key.len());
                assert_eq!(*value_size, value.len());
                // check if insertable
                let insert_data_size = key.len() + value.len();
                let remain_data_size = PAGE_SIZE as usize - (HEADER_SIZE + (key_size + value_size) * size);
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

                // TODO: 移動
                // let offset = HEADER_SIZE + key_size * i;
                // self.copy_within(src, dest);

                // TODO: insert
                false
            },
            Meta {
                key_size: Some(key_size),
                value_size: None,
            } => {
                // for i in 0..size {
                //     let offset = HEADER_SIZE + (key_size + INDEX_SIZE) * i;
                //     let k = &self[offset..offset + key_size];
                //     if key.as_slice() < k {
                //         return Some(i);
                //     }
                // }
                // None
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
                    if key.as_slice() == k {
                        return Some(i);
                    }
                }
                None
            },
            Meta {
                key_size: Some(key_size),
                value_size: None,
            } => {
                for i in 0..size {
                    let offset = HEADER_SIZE + (key_size + INDEX_SIZE) * i;
                    let k = &self[offset..offset + key_size];
                    if key.as_slice() == k {
                        return Some(i);
                    }
                }
                None
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
        todo!()
    }

    fn cursor_next(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> Self::Cursor {
        todo!()
    }

    fn cursor_is_end(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> bool {
        todo!()
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
