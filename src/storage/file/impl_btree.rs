use std::convert::TryInto;

use crate::btree::{BTree, BTreeNode};

use super::{
    page::Page,
    pager::{Pager, PAGE_SIZE},
};

const INTERNAL_HEADER_SIZE: usize = 1 + 4 + 2;
const LEAF_HEADER_SIZE: usize = 1 + 4 + 2 + 4;
const INDEX_SIZE: usize = 2;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

// # internal node layout
// [1] leaf flag // TODO なくしたい
// [4] parent node id
// [2] size
//
// ## key_size fixed
// key..., ...child
//
// ## key_size variable
// key_index..., ...(child, key), child
//
//
// # leaf node layout
// [1] leaf flag // TODO なくしたい
// [4] parent node id
// [2] size
// [4] next node id
// ...
//
// ## key_size fixed   , value_size fixed
// key...                         , ...value
//
// ## key_size fixed   , value_size variable
// (key, value_index)...          , ...value
//
// ## key_size variable, value_size fixed
// key_index...                   , ...(key, value)
//
// ## key_size variable, value_size variable
// (key_index, value_index)...    , ...(key, value)

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
        parse_u16(self.slice(1 + 4, 2)) as usize
    }

    fn split_out(&mut self, meta: &Self::Meta) -> (Key, Self) {
        let size = self.size(&meta);
        if self.is_leaf(meta) {
            let mut new_page = Page::new_leaf();

            let (pivot_index, pivot_key) = match meta {
                Meta {
                    key_size: Some(key_size),
                    value_size: Some(value_size),
                } => {
                    let (pivot_index, pivot_key) = {
                        let mut index = size / 2;
                        let key = self.slice(LEAF_HEADER_SIZE + key_size * index, *key_size);
                        for i in (0..index).rev() {
                            let k = self.slice(LEAF_HEADER_SIZE + key_size * i, *key_size);
                            if key != k {
                                index = i + 1;
                                break;
                            }
                        }
                        (index, key.to_vec())
                    };

                    // move keys
                    new_page[LEAF_HEADER_SIZE..LEAF_HEADER_SIZE + key_size * (size - pivot_index)]
                        .copy_from_slice(
                            &self[LEAF_HEADER_SIZE + key_size * pivot_index
                                ..LEAF_HEADER_SIZE + key_size * size],
                        );

                    // move values
                    new_page[PAGE_SIZE as usize - value_size * (size - pivot_index)
                        ..PAGE_SIZE as usize]
                        .copy_from_slice(
                            &self[PAGE_SIZE as usize - value_size * size
                                ..PAGE_SIZE as usize - value_size * pivot_index],
                        );

                    (pivot_index, pivot_key)
                }
                Meta {
                    key_size: Some(key_size),
                    value_size: None,
                } => {
                    let key_interval = key_size + INDEX_SIZE;

                    let (pivot_index, pivot_key) = {
                        let mut index = size / 2;
                        let key = self.slice(LEAF_HEADER_SIZE + key_interval * index, *key_size);
                        for i in (0..index).rev() {
                            let k = self.slice(LEAF_HEADER_SIZE + key_interval * i, *key_size);
                            if key != k {
                                index = i + 1;
                                break;
                            }
                        }
                        (index, key.to_vec())
                    };

                    let offset = LEAF_HEADER_SIZE + key_interval * (size - 1) + key_size;
                    let last_value_index = parse_u16(&self[offset..offset + INDEX_SIZE]) as usize;
                    let offset = LEAF_HEADER_SIZE + key_interval * (pivot_index - 1) + key_size; //?
                    let pivot_value_index = parse_u16(&self[offset..offset + INDEX_SIZE]) as usize;

                    // move keys
                    new_page
                        [LEAF_HEADER_SIZE..LEAF_HEADER_SIZE + key_interval * (size - pivot_index)]
                        .copy_from_slice(
                            &self[LEAF_HEADER_SIZE + key_interval * pivot_index
                                ..LEAF_HEADER_SIZE + key_interval * size],
                        );

                    // move values
                    let values_to_move_size = pivot_value_index - last_value_index;
                    new_page[PAGE_SIZE as usize - values_to_move_size..PAGE_SIZE as usize]
                        .copy_from_slice(&self[last_value_index..pivot_value_index]);

                    // recalculate value_index
                    let value_index_diff = PAGE_SIZE as u16 - pivot_value_index as u16;
                    for i in 0..(size - pivot_index) {
                        let offset = LEAF_HEADER_SIZE + key_interval * i + key_size;
                        let s = parse_u16(&new_page[offset..offset + INDEX_SIZE]);
                        new_page[offset..offset + INDEX_SIZE]
                            .copy_from_slice(&(s + value_index_diff).to_le_bytes());
                    }

                    (pivot_index, pivot_key)
                }
                Meta {
                    key_size: None,
                    value_size: Some(value_size),
                } => {
                    let key_interval = INDEX_SIZE;

                    let key_iter = |this: &mut Self, mut index: usize| {
                        let mut last_offset = parse_u16(
                            this.slice(LEAF_HEADER_SIZE + key_interval * index, key_interval),
                        ) as usize;
                        move |this: &mut Self| {
                            let offset =
                                if index == 0 {
                                    PAGE_SIZE as usize
                                } else {
                                    index -= 1;
                                    parse_u16(this.slice(
                                        LEAF_HEADER_SIZE + key_interval * index,
                                        key_interval,
                                    )) as usize
                                };
                            let range = last_offset..offset - value_size;
                            last_offset = offset;
                            range
                        }
                    };
                    let (pivot_index, pivot_key) = {
                        let mut index = size / 2;
                        let mut iter = key_iter(self, index);
                        let key = iter(self);
                        for i in (0..index).rev() {
                            let k = iter(self);
                            if self[key.clone()] != self[k] {
                                index = i + 1;
                                break;
                            }
                        }
                        (index, self[key].to_vec())
                    };

                    let offset = LEAF_HEADER_SIZE + key_interval * pivot_index;
                    let key_index_end = parse_u16(&self[offset - key_interval..offset]) as usize;

                    // move key_indexs
                    new_page
                        [LEAF_HEADER_SIZE..LEAF_HEADER_SIZE + key_interval * (size - pivot_index)]
                        .copy_from_slice(
                            &self[LEAF_HEADER_SIZE + key_interval * pivot_index
                                ..LEAF_HEADER_SIZE + key_interval * size],
                        );

                    let offset = LEAF_HEADER_SIZE + key_interval * (size - 1);
                    let last_value_index = parse_u16(&self[offset..offset + INDEX_SIZE]) as usize;

                    // move values
                    let values_to_move_size = key_index_end - last_value_index; // key_index_start は key_index_endが正しかった
                    new_page[PAGE_SIZE as usize - values_to_move_size..PAGE_SIZE as usize]
                        .copy_from_slice(&self[last_value_index..key_index_end]);

                    // recalculate value_index
                    let value_index_diff = PAGE_SIZE as u16 - key_index_end as u16;
                    for i in 0..(size - pivot_index) {
                        let slice =
                            new_page.slice_mut(LEAF_HEADER_SIZE + key_interval * i, INDEX_SIZE);
                        let s = parse_u16(slice);
                        slice.copy_from_slice(&(s + value_index_diff).to_le_bytes());
                    }

                    (pivot_index, pivot_key)
                }
                Meta {
                    key_size: None,
                    value_size: None,
                } => {
                    panic!("untested");
                    let pivot_index = 0;

                    let key_interval = INDEX_SIZE * 2;
                    let offset = LEAF_HEADER_SIZE + key_interval * pivot_index;
                    let key_index_start = parse_u16(&self[offset..offset + INDEX_SIZE]) as usize;
                    let key_index_end =
                        parse_u16(&self[offset + INDEX_SIZE..offset + key_interval]) as usize;
                    let pivot_key = self[key_index_start..key_index_end].to_vec();

                    // move (key_index, value_index)
                    new_page
                        [LEAF_HEADER_SIZE..LEAF_HEADER_SIZE + key_interval * (size - pivot_index)]
                        .copy_from_slice(
                            &self[LEAF_HEADER_SIZE + key_interval * pivot_index
                                ..LEAF_HEADER_SIZE + key_interval * size],
                        );

                    let offset = LEAF_HEADER_SIZE + key_interval * (size - 1);
                    let last_key_index = parse_u16(&self[offset..offset + INDEX_SIZE]) as usize;

                    // move values
                    let key_index = parse_u16(&self[offset - INDEX_SIZE..offset]) as usize;
                    let to_move_size = key_index - last_key_index;
                    new_page[PAGE_SIZE as usize - to_move_size..PAGE_SIZE as usize]
                        .copy_from_slice(&self[last_key_index..key_index]);

                    // recalculate key_index and value_index
                    let value_index_diff = PAGE_SIZE as u16 - key_index as u16;
                    for i in 0..(size - pivot_index) * 2 {
                        let slice =
                            new_page.slice_mut(LEAF_HEADER_SIZE + INDEX_SIZE * i, INDEX_SIZE);
                        let s = parse_u16(slice);
                        slice.copy_from_slice(&(s + value_index_diff).to_le_bytes());
                    }

                    (pivot_index, pivot_key)
                }
            };

            // rewrite size
            self.set_size(pivot_index);
            new_page.set_size(size - pivot_index);
            (pivot_key, new_page)
        } else {
            let mut new_page = Page::new_internal(meta);

            let value_size = 4;
            let (pivot_index, pivot_key) = match meta.key_size { // pivot_indexというよりはsizeを表している
                Some(key_size) => {
                    let (pivot_index, pivot_key) = {
                        let mut index = (size + 1) / 2;
                        let key = self.slice(INTERNAL_HEADER_SIZE + key_size * (index-1), key_size);
                        for i in (0..index-1).rev() {
                            let k = self.slice(INTERNAL_HEADER_SIZE + key_size * i, key_size);
                            if key != k {
                                index = i + 2;
                                break;
                            }
                        }
                        (index, key.to_vec())
                    };

                    // move keys
                    new_page[INTERNAL_HEADER_SIZE
                        ..INTERNAL_HEADER_SIZE + key_size * (size - 1 - (pivot_index))]
                        .copy_from_slice(
                            &self[INTERNAL_HEADER_SIZE + key_size * (pivot_index)
                                ..INTERNAL_HEADER_SIZE + key_size * (size - 1)],
                        );

                    // move values
                    new_page
                        [PAGE_SIZE as usize - value_size * (size - pivot_index)..PAGE_SIZE as usize]
                        .copy_from_slice(
                            &self[PAGE_SIZE as usize - value_size * (size)
                                ..PAGE_SIZE as usize - value_size * pivot_index],
                        );

                    (pivot_index, pivot_key)
                }
                None => {
                    // untested yet
                    panic!("untested");
                    let key_interval = INDEX_SIZE;

                    let key_iter = |this: &mut Self, mut index: usize| {
                        let mut last_offset = parse_u16(
                            this.slice(INTERNAL_HEADER_SIZE + key_interval * index, key_interval),
                        ) as usize;
                        move |this: &mut Self| {
                            let offset =
                                if index == 0 {
                                    PAGE_SIZE as usize
                                } else {
                                    index -= 1;
                                    parse_u16(this.slice(
                                        INTERNAL_HEADER_SIZE + key_interval * index,
                                        key_interval,
                                    )) as usize
                                };
                            let range = last_offset..offset - value_size;
                            last_offset = offset;
                            range
                        }
                    };
                    let (pivot_index, pivot_key) = {
                        let mut index = (size + 1) / 2;
                        let mut iter = key_iter(self, index-1);
                        let key = iter(self);
                        for i in (0..index-1).rev() {
                            let k = iter(self);
                            if self[key.clone()] != self[k] {
                                index = i + 2;
                                break;
                            }
                        }
                        (index, self[key].to_vec())
                    };

                    let offset = INTERNAL_HEADER_SIZE + key_interval * (pivot_index - 1);
                    let key_index_start = parse_u16(&self[offset..offset + key_interval]) as usize;
                    
                    // move key_indexs
                    new_page[INTERNAL_HEADER_SIZE
                        ..INTERNAL_HEADER_SIZE + key_interval * (size - 1 - pivot_index)]
                        .copy_from_slice(
                            &self[INTERNAL_HEADER_SIZE + key_interval * pivot_index
                                ..INTERNAL_HEADER_SIZE + key_interval * (size - 1)],
                        );

                    let offset = INTERNAL_HEADER_SIZE + key_interval * (size - 2);
                    let last_value_index = parse_u16(&self[offset..offset + INDEX_SIZE]) as usize;

                    // move values
                    let values_to_move_size = key_index_start - (last_value_index - value_size);
                    new_page[PAGE_SIZE as usize - values_to_move_size..PAGE_SIZE as usize]
                        .copy_from_slice(&self[last_value_index - value_size..key_index_start]);

                    // recalculate value_index
                    let value_index_diff = PAGE_SIZE as u16 - key_index_start as u16;
                    for i in 0..(size - pivot_index - 1) {
                        let slice =
                            new_page.slice_mut(INTERNAL_HEADER_SIZE + key_interval * i, INDEX_SIZE);
                        let s = parse_u16(slice);
                        slice.copy_from_slice(&(s + value_index_diff).to_le_bytes());
                    }

                    (pivot_index, pivot_key)
                }
            };
            // rewrite size
            self.set_size(pivot_index);
            new_page.set_size(size - pivot_index);
            (pivot_key, new_page)
        }
    }

    fn insert_node(&mut self, meta: &Self::Meta, key: &Key, node_i: usize) -> bool {
        let size = self.size(meta);
        let value_size = 4;
        match meta.key_size {
            Some(key_size) => {
                // check if insertable
                let insert_data_size = key_size + value_size;
                let remain_data_size = PAGE_SIZE as usize
                    - (INTERNAL_HEADER_SIZE + key_size * (size - 1) + value_size * size);
                if insert_data_size > remain_data_size {
                    return false;
                }

                // find insert index
                let mut insert_index = size - 1;
                for i in 0..size - 1 {
                    let offset = INTERNAL_HEADER_SIZE + key_size * i;
                    let k = &self[offset..offset + key_size];
                    if key.as_slice() < k {
                        insert_index = i;
                        break;
                    }
                }

                // move forward keys and values
                let key_offset = INTERNAL_HEADER_SIZE + key_size * insert_index;
                self.copy_within(
                    key_offset..INTERNAL_HEADER_SIZE + key_size * (size - 1),
                    key_offset + key_size,
                );
                let values_end = PAGE_SIZE as usize - value_size * size;
                let value_offset = PAGE_SIZE as usize - value_size * (insert_index + 1);
                self.copy_within(values_end..value_offset, values_end - value_size);

                // insert
                self[key_offset..key_offset + key_size].copy_from_slice(key);
                self[value_offset - value_size..value_offset]
                    .copy_from_slice(&(node_i as u32).to_le_bytes());
            }
            None => {
                // check if insertable
                let last_key_index = parse_u16(
                    &self.slice(INTERNAL_HEADER_SIZE + INDEX_SIZE * (size - 2), INDEX_SIZE),
                ) as usize;

                let insert_data_size = key.len() + value_size + INDEX_SIZE;
                let remain_data_size =
                    last_key_index - value_size - (INTERNAL_HEADER_SIZE + INDEX_SIZE * (size - 1));
                if insert_data_size > remain_data_size {
                    return false;
                }

                // find insert index
                let mut last_offset = PAGE_SIZE as usize;
                let mut insert_index = size - 1;
                for i in 0..size - 1 {
                    let offset =
                        parse_u16(self.slice(INTERNAL_HEADER_SIZE + INDEX_SIZE * i, INDEX_SIZE))
                            as usize;
                    let k = &self[offset..last_offset - value_size];
                    if key.as_slice() < k {
                        insert_index = i;
                        break;
                    }
                    last_offset = offset;
                }

                // recalculate key_index
                for i in (insert_index..size - 1).rev() {
                    let slice_from =
                        self.slice_mut(INTERNAL_HEADER_SIZE + INDEX_SIZE * i, INDEX_SIZE);
                    let key_index = parse_u16(slice_from);
                    let slice_to =
                        self.slice_mut(INTERNAL_HEADER_SIZE + INDEX_SIZE * (i + 1), INDEX_SIZE);
                    slice_to.copy_from_slice(
                        &(key_index - (key.len() + value_size) as u16).to_le_bytes(),
                    )
                }
                self.slice_mut(INTERNAL_HEADER_SIZE + INDEX_SIZE * insert_index, INDEX_SIZE)
                    .copy_from_slice(
                        &((last_offset - (key.len() + value_size)) as u16).to_le_bytes(),
                    );

                // move forward keys and values
                self.copy_within(
                    last_key_index - value_size..last_offset,
                    last_key_index - value_size - (key.len() + value_size),
                );

                // insert
                let offset = last_offset - value_size - key.len();
                self.slice_mut(offset, key.len()).copy_from_slice(key);
                self[offset - value_size..offset].copy_from_slice(&(node_i as u32).to_le_bytes());
            }
        };
        // increment size
        self.set_size(size + 1);
        true
    }

    fn get_child(&self, meta: &Self::Meta, key: &Key) -> usize {
        let size = self.size(meta);
        assert!(0 < size);

        let value_size = 4;
        match meta.key_size {
            Some(key_size) => {
                // TODO: binary search
                for i in 0..size - 1 {
                    let offset = INTERNAL_HEADER_SIZE + key_size * i;
                    let k = &self[offset..offset + key_size];
                    if key.as_slice() < k {
                        let offset = PAGE_SIZE as usize - value_size * (i + 1);
                        return parse_u32(&self[offset..offset + value_size]) as usize;
                    }
                }
                let offset = PAGE_SIZE as usize - value_size * size;
                parse_u32(&self[offset..offset + value_size]) as usize
            }
            None => {
                let mut last_offset = PAGE_SIZE as usize;
                for i in 0..(size - 1) {
                    let offset =
                        parse_u16(self.slice(INTERNAL_HEADER_SIZE + INDEX_SIZE * i, INDEX_SIZE))
                            as usize;
                    let k = &self[offset..last_offset - value_size];
                    if key.as_slice() < k {
                        break;
                    }
                    last_offset = offset;
                }
                parse_u32(self.slice(last_offset - value_size, value_size)) as usize
            }
        }
    }

    fn get_first_child(&self, meta: &Self::Meta) -> usize {
        let value_size = 4;
        match meta.key_size {
            Some(_) | None => {
                return parse_u32(&self.slice(PAGE_SIZE as usize - value_size, value_size))
                    as usize;
            }
        }
    }

    fn get_children(&self, meta: &Self::Meta) -> Vec<usize> {
        let size = self.size(meta);
        let value_size = 4;
        match meta.key_size {
            Some(_) => {
                let mut node_is = Vec::with_capacity(size);
                for i in 0..size {
                    let offset = PAGE_SIZE as usize - value_size * (i + 1);
                    node_is.push(parse_u32(self.slice(offset, value_size)) as usize);
                }
                node_is
            }
            None => {
                let mut node_is = Vec::with_capacity(size);
                node_is.push(
                    parse_u32(self.slice(PAGE_SIZE as usize - value_size, value_size)) as usize,
                );
                for i in 0..(size - 1) {
                    let offset = INTERNAL_HEADER_SIZE + INDEX_SIZE * i;
                    let key_index = parse_u16(self.slice(offset, INDEX_SIZE)) as usize;
                    node_is
                        .push(parse_u32(self.slice(key_index - value_size, value_size)) as usize);
                }
                node_is
            }
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
        match meta.key_size {
            Some(key_size) => {
                self.slice_mut(INTERNAL_HEADER_SIZE, key_size)
                    .copy_from_slice(key);
                self.slice_mut(PAGE_SIZE as usize - value_size, value_size)
                    .copy_from_slice(&(i1 as u32).to_le_bytes());
                self.slice_mut(PAGE_SIZE as usize - value_size * 2, value_size)
                    .copy_from_slice(&(i2 as u32).to_le_bytes());
            }
            None => {
                let key_index = PAGE_SIZE as usize - value_size - key.len();

                self.slice_mut(INTERNAL_HEADER_SIZE, INDEX_SIZE)
                    .copy_from_slice(&(key_index as u16).to_le_bytes());

                self.slice_mut(key_index, key.len()).copy_from_slice(key);
                self.slice_mut(PAGE_SIZE as usize - value_size, value_size)
                    .copy_from_slice(&(i1 as u32).to_le_bytes());
                self.slice_mut(key_index - value_size, value_size)
                    .copy_from_slice(&(i2 as u32).to_le_bytes());
            }
        }
    }

    fn insert_value(&mut self, meta: &Self::Meta, key: &Key, value: &Value) -> bool {
        let size = self.size(meta);
        let res = match meta {
            Meta {
                key_size: Some(key_size),
                value_size: Some(value_size),
            } => {
                assert_eq!(*key_size, key.len());
                assert_eq!(*value_size, value.len());

                // check if insertable
                let insert_data_size = *key_size + *value_size;
                let remain_data_size =
                    PAGE_SIZE as usize - (LEAF_HEADER_SIZE + (key_size + value_size) * size);
                if insert_data_size > remain_data_size {
                    return false;
                }

                // find insert index
                let mut insert_index = size;
                for i in 0..size {
                    let offset = LEAF_HEADER_SIZE + key_size * i;
                    let k = &self[offset..offset + key_size];
                    if key.as_slice() < k {
                        insert_index = i;
                        break;
                    }
                }

                // move forward keys and values
                let key_offset = LEAF_HEADER_SIZE + key_size * insert_index;
                self.copy_within(
                    key_offset..LEAF_HEADER_SIZE + key_size * size,
                    key_offset + key_size,
                );
                let values_end = PAGE_SIZE as usize - value_size * size;
                let value_offset = PAGE_SIZE as usize - value_size * insert_index;
                self.copy_within(values_end..value_offset, values_end - value_size);

                // insert
                self[key_offset..key_offset + key_size].copy_from_slice(key);
                self[value_offset - value_size..value_offset].copy_from_slice(value);

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
                    let last_value_index_index =
                        LEAF_HEADER_SIZE + key_interval * size - INDEX_SIZE;
                    parse_u16(&self[last_value_index_index..last_value_index_index + INDEX_SIZE])
                        as usize
                };
                let insert_data_size = key_interval + value.len();
                let remain_data_size = last_value_index - (LEAF_HEADER_SIZE + key_interval * size);
                if insert_data_size > remain_data_size {
                    return false;
                }

                // find insert index
                let mut insert_index = size;
                for i in 0..size {
                    let offset = LEAF_HEADER_SIZE + key_interval * i;
                    let k = &self[offset..offset + key_size];
                    if key.as_slice() < k {
                        insert_index = i;
                        break;
                    }
                }

                // move forward keys and values
                let key_offset = LEAF_HEADER_SIZE + key_interval * insert_index;
                self.copy_within(
                    key_offset..LEAF_HEADER_SIZE + key_interval * size,
                    key_offset + key_interval,
                );
                let end = if insert_index == 0 {
                    PAGE_SIZE as usize
                } else {
                    parse_u16(&self[key_offset - INDEX_SIZE..key_offset]) as usize
                };
                self.copy_within(last_value_index..end, last_value_index - value.len());

                // recalculate value_index
                for i in (insert_index..size).rev() {
                    let offset = LEAF_HEADER_SIZE + key_interval * (i + 1) + key_size;
                    let s = parse_u16(&self[offset..offset + INDEX_SIZE]);
                    self[offset..offset + INDEX_SIZE]
                        .copy_from_slice(&(s - value.len() as u16).to_le_bytes());
                }
                let value_index_offset = LEAF_HEADER_SIZE + key_interval * insert_index + key_size;
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
            } => {
                // check if insertable
                let last_value_offset = if size == 0 {
                    PAGE_SIZE as usize
                } else {
                    parse_u16(&self.slice(LEAF_HEADER_SIZE + INDEX_SIZE * (size - 1), INDEX_SIZE))
                        as usize
                };

                let insert_data_size = INDEX_SIZE + key.len() + *value_size;
                let remain_data_size = last_value_offset - (LEAF_HEADER_SIZE + INDEX_SIZE * size);
                if insert_data_size > remain_data_size {
                    return false;
                }

                // find insert index
                let mut last_offset = PAGE_SIZE as usize;
                let mut insert_index = size;
                for i in 0..size {
                    let offset =
                        parse_u16(self.slice(LEAF_HEADER_SIZE + INDEX_SIZE * i, INDEX_SIZE))
                            as usize;
                    let k = &self[offset..last_offset - value_size];
                    if key.as_slice() < k {
                        insert_index = i;
                        break;
                    }
                    last_offset = offset;
                }

                // recalculate key_index
                for i in (insert_index..size).rev() {
                    let slice_from = self.slice_mut(LEAF_HEADER_SIZE + INDEX_SIZE * i, INDEX_SIZE);
                    let key_index = parse_u16(slice_from);
                    let slice_to =
                        self.slice_mut(LEAF_HEADER_SIZE + INDEX_SIZE * (i + 1), INDEX_SIZE);
                    slice_to.copy_from_slice(
                        &(key_index - (key.len() + *value_size) as u16).to_le_bytes(),
                    )
                }
                self.slice_mut(LEAF_HEADER_SIZE + INDEX_SIZE * insert_index, INDEX_SIZE)
                    .copy_from_slice(
                        &((last_offset - value_size - key.len()) as u16).to_le_bytes(),
                    );

                // move forward keys and values
                self.copy_within(
                    last_value_offset..last_offset,
                    last_value_offset - (key.len() + *value_size),
                );

                // insert
                self.slice_mut(last_offset - value_size - key.len(), key.len())
                    .copy_from_slice(key);
                self[last_offset - value_size..last_offset].copy_from_slice(value);

                true
            }
            Meta {
                key_size: None,
                value_size: None,
            } => {
                let key_interval = INDEX_SIZE * 2;
                panic!("untested");
                // check if insertable
                let last_value_offset = if size == 0 {
                    PAGE_SIZE as usize
                } else {
                    parse_u16(&self.slice(LEAF_HEADER_SIZE + key_interval * (size - 1), INDEX_SIZE))
                        as usize
                };

                let insert_data_size = INDEX_SIZE * 2 + key.len() + value.len();
                let remain_data_size = last_value_offset - (LEAF_HEADER_SIZE + key_interval * size);
                if insert_data_size > remain_data_size {
                    return false;
                }

                // find insert index
                let mut insert_offset = PAGE_SIZE as usize;
                let mut insert_index = size;
                for i in 0..size {
                    let key_offset =
                        parse_u16(self.slice(LEAF_HEADER_SIZE + key_interval * i, INDEX_SIZE))
                            as usize;
                    let value_offset = parse_u16(
                        self.slice(LEAF_HEADER_SIZE + key_interval * i + INDEX_SIZE, INDEX_SIZE),
                    ) as usize;
                    let k = &self[key_offset..value_offset];
                    if key.as_slice() < k {
                        insert_index = i;
                        break;
                    }
                    insert_offset = key_offset;
                }

                // recalculate key_index
                for i in (insert_index * 2..size * 2).rev() {
                    let slice_from = self.slice_mut(LEAF_HEADER_SIZE + INDEX_SIZE * i, INDEX_SIZE);
                    let key_index = parse_u16(slice_from);
                    let slice_to =
                        self.slice_mut(LEAF_HEADER_SIZE + INDEX_SIZE * (i + 1), INDEX_SIZE);
                    slice_to.copy_from_slice(
                        &(key_index + (key.len() + value.len()) as u16).to_le_bytes(),
                    )
                }
                self.slice_mut(LEAF_HEADER_SIZE + key_interval * insert_index, INDEX_SIZE)
                    .copy_from_slice(
                        &((insert_offset - value.len() - key.len()) as u16).to_le_bytes(),
                    );
                self.slice_mut(
                    LEAF_HEADER_SIZE + key_interval * insert_index + INDEX_SIZE,
                    INDEX_SIZE,
                )
                .copy_from_slice(&((insert_offset - value.len()) as u16).to_le_bytes());

                // move forward keys and values
                self.copy_within(
                    last_value_offset..insert_offset,
                    last_value_offset - (key.len() + value.len()),
                );

                // insert
                self.slice_mut(insert_offset - value.len() - key.len(), key.len())
                    .copy_from_slice(key);
                self[insert_offset - value.len()..insert_offset].copy_from_slice(value);

                true
            }
        };
        if res {
            // increment size
            self.set_size(size + 1);
        }
        res
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
                key_size: Some(key_size),
                value_size: Some(_),
            } => {
                for i in 0..size {
                    let offset = LEAF_HEADER_SIZE + key_size * i;
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
                    let offset = LEAF_HEADER_SIZE + (key_size + INDEX_SIZE) * i;
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
            } => {
                let mut last_offset = PAGE_SIZE as usize;
                for i in 0..size {
                    let offset =
                        parse_u16(self.slice(LEAF_HEADER_SIZE + INDEX_SIZE * i, INDEX_SIZE))
                            as usize;
                    let k = &self[offset..last_offset - value_size];
                    if key.as_slice() <= k {
                        return Some(i);
                    }
                    last_offset = offset;
                }
                Some(size)
            }
            Meta {
                key_size: None,
                value_size: None,
            } => {
                let key_intarval = INDEX_SIZE * 2;
                for i in 0..size {
                    let key_index =
                        parse_u16(self.slice(LEAF_HEADER_SIZE + key_intarval * i, INDEX_SIZE))
                            as usize;
                    let value_index = parse_u16(
                        self.slice(LEAF_HEADER_SIZE + key_intarval * i + INDEX_SIZE, INDEX_SIZE),
                    ) as usize;
                    let k = &self[key_index..value_index];
                    if key.as_slice() <= k {
                        return Some(i);
                    }
                }
                Some(size)
            }
        }
    }

    fn cursor_get(&self, meta: &Self::Meta, cursor: usize) -> Option<(Key, Value)> {
        match meta {
            Meta {
                key_size: Some(key_size),
                value_size: Some(value_size),
            } => {
                let key_index = LEAF_HEADER_SIZE + key_size * cursor;
                let key = self[key_index..key_index + key_size].to_vec();
                let value_index = PAGE_SIZE as usize - value_size * (cursor + 1);
                let value = self[value_index..value_index + value_size].to_vec();

                Some((key, value))
            }
            Meta {
                key_size: Some(key_size),
                value_size: None,
            } => {
                let key_interval = key_size + INDEX_SIZE;
                let value_index_index = LEAF_HEADER_SIZE + key_interval * cursor + key_size;
                let value_start =
                    parse_u16(&self[value_index_index..value_index_index + INDEX_SIZE]) as usize;
                let value_end = if cursor == 0 {
                    PAGE_SIZE as usize
                } else {
                    let value_index_index =
                        LEAF_HEADER_SIZE + key_interval * (cursor - 1) + key_size;
                    parse_u16(&self[value_index_index..value_index_index + INDEX_SIZE]) as usize
                };
                let key_index = LEAF_HEADER_SIZE + key_interval * cursor;
                let key = self[key_index..key_index + key_size].to_vec();
                let value = self[value_start..value_end].to_vec();

                Some((key, value))
            }
            Meta {
                key_size: None,
                value_size: Some(value_size),
            } => {
                let end_offset = if cursor == 0 {
                    PAGE_SIZE as usize
                } else {
                    parse_u16(self.slice(LEAF_HEADER_SIZE + INDEX_SIZE * (cursor - 1), INDEX_SIZE))
                        as usize
                };
                let start_offset =
                    parse_u16(self.slice(LEAF_HEADER_SIZE + INDEX_SIZE * cursor, INDEX_SIZE))
                        as usize;

                let key = self[start_offset..end_offset - value_size].to_vec();
                let value = self.slice(end_offset - value_size, *value_size).to_vec();

                Some((key, value))
            }
            Meta {
                key_size: None,
                value_size: None,
            } => {
                let key_interval = INDEX_SIZE * 2;
                let key_index1 = if cursor == 0 {
                    PAGE_SIZE as usize
                } else {
                    parse_u16(
                        self.slice(LEAF_HEADER_SIZE + key_interval * (cursor - 1), INDEX_SIZE),
                    ) as usize
                };
                let key_index2 =
                    parse_u16(self.slice(LEAF_HEADER_SIZE + key_interval * cursor, INDEX_SIZE))
                        as usize;
                let value_index = parse_u16(self.slice(
                    LEAF_HEADER_SIZE + key_interval * cursor + INDEX_SIZE,
                    INDEX_SIZE,
                )) as usize;

                let key = self[key_index2..value_index].to_vec();
                let value = self[value_index..key_index1].to_vec();

                Some((key, value))
            }
        }
    }

    fn cursor_delete(&mut self, meta: &Self::Meta, cursor: usize) -> bool {
        let size = self.size(meta);
        assert!(0 < size);
        assert!(cursor < size);
        match meta {
            Meta {
                key_size: Some(key_size),
                value_size: Some(value_size),
            } => {
                // move forward keys and values
                self.copy_within(
                    PAGE_SIZE as usize - value_size * size
                        ..PAGE_SIZE as usize - value_size * (cursor + 1),
                    PAGE_SIZE as usize - value_size * size + value_size,
                );
                self.copy_within(
                    LEAF_HEADER_SIZE + key_size * (cursor + 1)..LEAF_HEADER_SIZE + key_size * size,
                    LEAF_HEADER_SIZE + key_size * cursor,
                );
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
                        LEAF_HEADER_SIZE + key_interval * (cursor - 1) + key_size,
                        INDEX_SIZE,
                    ))
                };
                let value_index2 = parse_u16(self.slice(
                    LEAF_HEADER_SIZE + key_interval * cursor + key_size,
                    INDEX_SIZE,
                ));
                let last_value_index = parse_u16(self.slice(
                    LEAF_HEADER_SIZE + key_interval * (size - 1) + key_size,
                    INDEX_SIZE,
                ));

                // move forward keys and values
                self.copy_within(
                    last_value_index as usize..value_index2 as usize,
                    (last_value_index + (value_index1 - value_index2)) as usize,
                );
                self.copy_within(
                    LEAF_HEADER_SIZE + key_interval * (cursor + 1)
                        ..LEAF_HEADER_SIZE + key_interval * size,
                    LEAF_HEADER_SIZE + key_interval * cursor,
                );

                // recalculate value_index
                for i in cursor..size - 1 {
                    let offset = LEAF_HEADER_SIZE + key_interval * i + key_size;
                    let slice = self.slice_mut(offset, INDEX_SIZE);
                    let value_index = parse_u16(slice);
                    slice.copy_from_slice(
                        &(value_index + (value_index1 - value_index2)).to_le_bytes(),
                    )
                }
            }
            Meta {
                key_size: None,
                value_size: Some(_),
            } => {
                let key_interval = INDEX_SIZE;

                let last_value_offset = parse_u16(
                    &self.slice(LEAF_HEADER_SIZE + key_interval * (size - 1), INDEX_SIZE),
                ) as usize;

                let key_index1 = if cursor == 0 {
                    PAGE_SIZE as u16
                } else {
                    parse_u16(
                        self.slice(LEAF_HEADER_SIZE + key_interval * (cursor - 1), INDEX_SIZE),
                    )
                };
                let key_index2 =
                    parse_u16(self.slice(LEAF_HEADER_SIZE + key_interval * cursor, INDEX_SIZE));

                // recalculate key_index
                for i in (cursor + 1)..size {
                    let slice_from =
                        self.slice_mut(LEAF_HEADER_SIZE + key_interval * i, INDEX_SIZE);
                    let key_index = parse_u16(slice_from);
                    let slice_to =
                        self.slice_mut(LEAF_HEADER_SIZE + key_interval * (i - 1), INDEX_SIZE);
                    slice_to.copy_from_slice(&(key_index + (key_index1 - key_index2)).to_le_bytes())
                }

                // move forward keys and values
                self.copy_within(
                    last_value_offset..key_index2 as usize,
                    last_value_offset + (key_index1 - key_index2) as usize,
                );
            }
            Meta {
                key_size: None,
                value_size: None,
            } => {
                let key_interval = INDEX_SIZE * 2;

                let last_value_offset = parse_u16(
                    &self.slice(LEAF_HEADER_SIZE + key_interval * (size - 1), INDEX_SIZE),
                ) as usize;

                let key_index1 = if cursor == 0 {
                    PAGE_SIZE as u16
                } else {
                    parse_u16(
                        self.slice(LEAF_HEADER_SIZE + key_interval * (cursor - 1), INDEX_SIZE),
                    )
                };
                let key_index2 =
                    parse_u16(self.slice(LEAF_HEADER_SIZE + key_interval * cursor, INDEX_SIZE));

                // recalculate key_index
                for i in (cursor + 1) * 2..size * 2 {
                    let slice_from = self.slice_mut(LEAF_HEADER_SIZE + INDEX_SIZE * i, INDEX_SIZE);
                    let key_index = parse_u16(slice_from);
                    let slice_to =
                        self.slice_mut(LEAF_HEADER_SIZE + INDEX_SIZE * (i - 1), INDEX_SIZE);
                    slice_to.copy_from_slice(&(key_index + (key_index1 - key_index2)).to_le_bytes())
                }

                // move forward keys and values
                self.copy_within(
                    last_value_offset..key_index2 as usize,
                    last_value_offset + (key_index1 - key_index2) as usize,
                );
            }
        };
        // dencrement size
        self.set_size(size - 1);
        true
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
