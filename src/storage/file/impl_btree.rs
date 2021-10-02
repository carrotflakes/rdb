use crate::{
    btree::{BTree, BTreeNode, Key},
    data::Data,
};

use super::{pager::PAGE_SIZE, File, Page};

pub struct Node {
    key_size: Option<usize>,
    value_size: Option<usize>,
    page: Page,
}

// [1] leaf flag
// [4] parent node id
// [2] size
// [4] next node id

pub struct Meta {
    pub key_size: Option<usize>,
    pub value_size: Option<usize>,
}

impl BTreeNode<Vec<Data>, Vec<Data>> for Page {
    type Meta = Meta;
    type Cursor = usize;

    fn is_leaf(&self, _: &Self::Meta) -> bool {
        self[0] == 1
    }

    fn get_parent(&self, _: &Self::Meta) -> Option<usize> {
        use std::convert::TryInto;
        let i = u32::from_le_bytes(self[1..1 + 4].as_ref().try_into().unwrap());
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
        use std::convert::TryInto;
        u16::from_le_bytes(self[1 + 4..1 + 4 + 2].as_ref().try_into().unwrap()) as usize
    }

    fn is_full(&self, meta: &Self::Meta) -> bool {
        debug_assert!(!self.is_leaf(meta));
        if let Some(key_size) = meta.key_size {
            // fixed key_size:
            let value_size = 4;
            let size = self.size(meta);
            let size_max = (PAGE_SIZE - (1+4+2+4)) / (key_size + value_size) as u64;
            size as u64 == size_max
        } else {
            // variable key_size:
            panic!("variable key_size is not supported")
        }
    }

    fn insert(&mut self, meta: &Self::Meta, key: &Vec<Data>, value: &Vec<Data>) -> bool {
        todo!()
    }

    fn insert_node(&mut self, meta: &Self::Meta, key: &Vec<Data>, node_i: usize) {
        todo!()
    }

    fn get(&self, meta: &Self::Meta, key: &Vec<Data>) -> Option<Vec<Data>> {
        todo!()
    }

    fn get_child(&self, meta: &Self::Meta, key: &Vec<Data>) -> usize {
        todo!()
    }

    fn get_first_child(&self, meta: &Self::Meta) -> usize {
        todo!()
    }

    fn get_children(&self, meta: &Self::Meta) -> Vec<usize> {
        todo!()
    }

    fn remove(&mut self, meta: &Self::Meta, key: &Vec<Data>) -> bool {
        todo!()
    }

    fn split_out(&mut self, meta: &Self::Meta) -> (Vec<Data>, Self) {
        todo!()
    }

    fn new_internal(meta: &Self::Meta) -> Self {
        [0; PAGE_SIZE as usize].into()
    }

    fn init_as_root(&mut self, meta: &Self::Meta, key: &Vec<Data>, i1: usize, i2: usize) {
        todo!()
    }

    fn first_cursor(&self, meta: &Self::Meta) -> Self::Cursor {
        0
    }

    fn find(&self, meta: &Self::Meta, key: &Vec<Data>) -> Option<Self::Cursor> {
        todo!()
    }

    fn cursor_get(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> Option<(Vec<Data>, Vec<Data>)> {
        todo!()
    }

    fn cursor_next(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> Self::Cursor {
        todo!()
    }

    fn cursor_is_end(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> bool {
        todo!()
    }
}

impl BTree<Vec<Data>, Vec<Data>> for File {
    type Node = Page;

    fn add_root_node(&mut self) -> usize {
        let page_i = self.pager.size();
        self.pager.get_ref(page_i);
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
