use std::convert::TryInto;

use crate::btree::{BTree, BTreeNode};

use super::{
    pager::{Pager, PAGE_SIZE},
    Page,
};

const HEADER_SIZE: usize = 1 + 4 + 2 + 4;
const INDEX_SIZE: usize = 2;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

// # internal node layout
// [1] leaf flag // TODO なくしたい
// [4] parent node id
// [2] size
//
// ## key_size 0
// child...
//
// ## key_size fixed
// key..., child...
//
// ## key_size variable
// (key_size, key)..., ...child
//
//
// # leaf node layout
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

    fn is_leaf(&self, _: &Self::Meta) -> bool {
        self[0] == 1
    }

    fn get_parent(&self, _: &Self::Meta) -> Option<usize> {
        let i = parse_u32(&self[1..1 + 4]);
        if i == 0 {
            None
        } else {
            Some(i as usize)
        }
    }

    fn set_parent(&mut self, _: &Self::Meta, i: usize) {
        self.set_parent(i);
    }

    fn size(&self, _: &Self::Meta) -> usize {
        parse_u16(&self[1 + 4..1 + 4 + 2]) as usize
    }

    fn split_out(&mut self, meta: &Self::Meta) -> (Key, Self) {
        let size = self.size(&meta);
        if self.is_leaf(meta) {
            // rewrite size
            let to_size = size / 2;
            self.set_size(to_size);

            let mut new_page = Page::new_leaf(None);
            new_page.set_size(size - to_size);

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
                    let offset = HEADER_SIZE + key_size * to_size;
                    let pivot_key = self[offset..offset + key_size].to_vec();

                    let capacity = (PAGE_SIZE as usize - HEADER_SIZE) / (key_size + value_size);
                    let values_offset = HEADER_SIZE + capacity * *key_size;

                    // move keys
                    new_page[HEADER_SIZE..HEADER_SIZE + key_size * (size - to_size)]
                        .copy_from_slice(
                            &self[HEADER_SIZE + key_size * to_size..HEADER_SIZE + key_size * size],
                        );

                    // move values
                    new_page[values_offset..values_offset + value_size * (size - to_size)]
                        .copy_from_slice(
                            &self[values_offset + value_size * to_size
                                ..values_offset + value_size * size],
                        );

                    (pivot_key, new_page)
                }
                Meta {
                    key_size: Some(key_size),
                    value_size: None,
                } => {
                    let key_interval = key_size + INDEX_SIZE;
                    let offset = HEADER_SIZE + key_interval * to_size;
                    let pivot_key = self[offset..offset + key_size].to_vec();

                    let offset = HEADER_SIZE + key_interval * size + key_size;
                    let last_value_index = parse_u16(&self[offset..offset + INDEX_SIZE]) as usize;
                    let offset = HEADER_SIZE + key_interval * (to_size - 1) + key_size; //?
                    let pivot_value_index = parse_u16(&self[offset..offset + INDEX_SIZE]) as usize;

                    // move keys
                    new_page[HEADER_SIZE..HEADER_SIZE + key_interval * (size - to_size)]
                        .copy_from_slice(
                            &self[HEADER_SIZE + key_interval * to_size
                                ..HEADER_SIZE + key_interval * size],
                        );

                    // move values
                    let values_to_move_size = pivot_value_index - last_value_index;
                    new_page[PAGE_SIZE as usize - values_to_move_size..PAGE_SIZE as usize]
                        .copy_from_slice(&self[last_value_index..pivot_value_index]);

                    // recalculate value_index
                    let value_index_diff = PAGE_SIZE as u16 - pivot_value_index as u16;
                    for i in 0..(size - to_size) {
                        let offset = HEADER_SIZE + key_interval * i + key_size;
                        let s = parse_u16(&new_page[offset..offset + INDEX_SIZE]);
                        new_page[offset..offset + INDEX_SIZE]
                            .copy_from_slice(&(s + value_index_diff).to_le_bytes());
                    }

                    (pivot_key, new_page)
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
        } else {
            todo!()
        }
    }

    fn is_full(&self, meta: &Self::Meta) -> bool {
        debug_assert!(!self.is_leaf(meta));
        if let Some(key_size) = meta.key_size {
            // fixed key_size:
            let value_size = 4;
            let size = self.size(meta);
            let size_max = (PAGE_SIZE as usize - HEADER_SIZE) / (key_size + value_size);
            size == size_max
        } else {
            // variable key_size:
            panic!("variable key_size is not supported")
        }
    }

    fn insert_node(&mut self, meta: &Self::Meta, key: &Key, node_i: usize) -> bool {
        let size = self.size(meta);
        let res = match meta {
            Meta {
                key_size: Some(0),
                value_size: _,
            } => todo!(),
            Meta {
                key_size: Some(key_size),
                value_size: _,
            } => {
                // check if insertable
                let insert_data_size = *key_size + INDEX_SIZE;
                let remain_data_size =
                    PAGE_SIZE as usize - (HEADER_SIZE + key_size * (size - 1) + INDEX_SIZE * size);
                if insert_data_size > remain_data_size {
                    return false;
                }

                // find insert index
                let mut insert_index = size - 1;
                for i in 0..size - 1 {
                    let offset = HEADER_SIZE + key_size * i;
                    let k = &self[offset..offset + key_size];
                    if key.as_slice() < k {
                        insert_index = i;
                        break;
                    }
                }

                let capacity = (PAGE_SIZE as usize - HEADER_SIZE + key_size) / insert_data_size;
                let values_offset = HEADER_SIZE + (capacity - 1) * *key_size;

                // move forward keys and values
                let key_offset = HEADER_SIZE + key_size * insert_index;
                self.copy_within(
                    key_offset..HEADER_SIZE + key_size * (size - 1),
                    key_offset + key_size,
                );
                let value_offset = values_offset + INDEX_SIZE * (insert_index + 1);
                self.copy_within(
                    value_offset..values_offset + INDEX_SIZE * size,
                    value_offset + INDEX_SIZE,
                );

                // insert
                self[key_offset..key_offset + key_size].copy_from_slice(key);
                self[value_offset..value_offset + INDEX_SIZE]
                    .copy_from_slice(&(node_i as u16).to_le_bytes());

                true
            }
            Meta {
                key_size: None,
                value_size: _,
            } => todo!(),
        };
        if res {
            // increment size
            self.set_size(size + 1);
        }
        res
    }

    fn get_child(&self, meta: &Self::Meta, key: &Key) -> usize {
        let size = self.size(meta);
        if size == 0 {
            return 0;
        }
        match &meta.key_size {
            Some(0) => 0,
            Some(key_size) => {
                // TODO: binary search
                let value_size = 4;
                let capacity = (PAGE_SIZE as usize - HEADER_SIZE) / (*key_size + value_size);
                let values_offset = HEADER_SIZE + capacity * *key_size;

                for i in 0..size - 1 {
                    let offset = HEADER_SIZE + key_size * i;
                    let k = &self[offset..offset + key_size];
                    if key.as_slice() < k {
                        let offset = values_offset + value_size * i;
                        return parse_u32(&self[offset..offset + value_size]) as usize;
                    }
                }
                let offset = values_offset + value_size * (size - 1);
                parse_u32(&self[offset..offset + value_size]) as usize
            }
            None => todo!(),
        }
    }

    fn get_first_child(&self, meta: &Self::Meta) -> usize {
        match &meta.key_size {
            Some(0) => todo!(),
            Some(key_size) => {
                let value_size = 4;
                let capacity = (PAGE_SIZE as usize - HEADER_SIZE) / (*key_size + value_size);
                let values_offset = HEADER_SIZE + capacity * *key_size;

                return parse_u32(&self[values_offset..values_offset + value_size]) as usize;
            }
            None => todo!(),
        }
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
                key_size: Some(_),
                value_size: _,
            } => {
                let mut node_is = Vec::with_capacity(size);
                for i in 0..size {
                    let offset = PAGE_SIZE as usize - 4 * (size - i);
                    node_is.push(parse_u32(&self[offset..offset + 4]) as usize);
                }
                node_is
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

    fn new_internal(_: &Self::Meta) -> Self {
        [0; PAGE_SIZE as usize].into()
    }

    fn init_as_root_internal(&mut self, meta: &Self::Meta, key: &Key, i1: usize, i2: usize) {
        self[0] = 0;
        self.set_parent(0);
        self.set_size(2);
        self.set_next(0);

        let value_size = 4;
        match meta {
            Meta {
                key_size: Some(0),
                value_size: _,
            } => todo!(),
            Meta {
                key_size: Some(key_size),
                value_size: _,
            } => {
                let capacity = (PAGE_SIZE as usize - HEADER_SIZE) / (*key_size + value_size);
                let values_offset = HEADER_SIZE + capacity * *key_size;

                self[HEADER_SIZE..HEADER_SIZE + key_size].copy_from_slice(key);
                self[values_offset..values_offset + value_size]
                    .copy_from_slice(&(i1 as u32).to_le_bytes());
                self[values_offset + value_size..values_offset + value_size * 2]
                    .copy_from_slice(&(i2 as u32).to_le_bytes());
            }
            Meta {
                key_size: None,
                value_size: _,
            } => todo!(),
        }
    }

    fn insert_value(&mut self, meta: &Self::Meta, key: &Key, value: &Value) -> bool {
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
                let insert_data_size = key_interval + value.len();
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
                    parse_u16(&self[key_offset - INDEX_SIZE..key_offset]) as usize
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
                    parse_u16(
                        &self[value_index_offset - key_interval
                            ..value_index_offset - key_interval + INDEX_SIZE],
                    )
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
            self.set_size(size + 1);
        }
        res
    }

    fn remove(&mut self, meta: &Self::Meta, key: &Key) -> bool {
        todo!()
    }

    fn get_next(&self, _: &Self::Meta) -> Option<usize> {
        let i = parse_u32(&self[1 + 4 + 2..1 + 4 + 2 + 4]) as usize;
        if i == 0 {
            None
        } else {
            Some(i)
        }
    }

    fn set_next(&mut self, _: &Self::Meta, i: usize) {
        self.set_next(i);
    }

    fn first_cursor(&self, _: &Self::Meta) -> usize {
        0
    }

    fn find(&self, meta: &Self::Meta, key: &Key) -> Option<usize> {
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

    fn cursor_get(&self, meta: &Self::Meta, cursor: usize) -> Option<(Key, Value)> {
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
                let value_end = if cursor == 0 {
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

    fn cursor_delete(&mut self, meta: &Self::Meta, cursor: usize) -> bool {
        let size = self.size(meta);
        assert!(0 < size);
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
                todo!();
            }
            Meta {
                key_size: Some(key_size),
                value_size: None,
            } => {
                let key_interval = key_size + INDEX_SIZE;

                let value_index1 = if cursor == 0 {
                    PAGE_SIZE as u16
                } else {
                    parse_u16(self.slice(
                        HEADER_SIZE + key_interval * (cursor - 1) + key_size,
                        INDEX_SIZE,
                    ))
                };
                let value_index2 = parse_u16(
                    self.slice(HEADER_SIZE + key_interval * cursor + key_size, INDEX_SIZE),
                );
                let last_value_index = parse_u16(self.slice(
                    HEADER_SIZE + key_interval * (size - 1) + key_size,
                    INDEX_SIZE,
                ));

                // move forward keys and values
                self.copy_within(
                    last_value_index as usize..value_index2 as usize,
                    (last_value_index + (value_index1 - value_index2)) as usize,
                );
                self.copy_within(
                    HEADER_SIZE + key_interval * (cursor + 1)..HEADER_SIZE + key_interval * size,
                    HEADER_SIZE + key_interval * cursor,
                );

                // recalculate value_index
                for i in cursor..size - 1 {
                    let offset = HEADER_SIZE + key_interval * i + key_size;
                    let value_index = parse_u16(self.slice(offset, INDEX_SIZE));
                    self.slice_mut(offset, INDEX_SIZE).copy_from_slice(
                        &(value_index + (value_index1 - value_index2)).to_le_bytes(),
                    )
                }

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
            // dencrement size
            self.set_size(size - 1);
        }
        res
    }
}

impl BTree<Key, Value> for Pager<Page> {
    type Node = Page;

    fn add_root_node(&mut self) -> usize {
        let page_i = self.size();
        let page = self.get_mut(page_i);

        page[0] = 1;
        page.set_parent(0);
        page.set_size(0);
        page.set_next(0);

        page_i
    }

    fn node_ref(&self, node_i: usize) -> &Self::Node {
        self.get_ref(node_i)
    }

    fn node_mut(&mut self, node_i: usize) -> &mut Self::Node {
        self.get_mut(node_i)
    }

    fn push(&mut self, node: Self::Node) -> usize {
        self.push(node)
    }

    fn swap(&mut self, node_i: usize, node: Self::Node) -> Self::Node {
        self.swap(node_i, node)
    }
}

fn parse_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().unwrap())
}

fn parse_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().unwrap())
}
