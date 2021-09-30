use crate::{
    btree::{BTree, BTreeNode},
    data::Data,
};

use super::{pager::Page, File};

impl BTreeNode<Data> for Page {
    fn is_leaf(&self) -> bool {
        self[0] == 1
    }

    fn get_parent(&self) -> Option<usize> {
        use std::convert::TryInto;
        let i = u32::from_le_bytes(self[1..1 + 4].as_ref().try_into().unwrap());
        if i == u32::MAX {
            None
        } else {
            Some(i as usize)
        }
    }

    fn set_parent(&mut self, i: usize) {
        self[1..1 + 4].copy_from_slice(&u32::to_le_bytes(i as u32))
    }

    fn size(&self) -> usize {
        todo!()
    }

    fn is_full(&self) -> bool {
        todo!()
    }

    fn insert(&mut self, key: usize, value: Data) {
        todo!()
    }

    fn insert_node(&mut self, key: usize, node_i: usize) {
        todo!()
    }

    fn get(&self, key: usize) -> Option<Data> {
        todo!()
    }

    fn get_child(&self, key: usize) -> usize {
        todo!()
    }

    fn get_children(&self) -> Vec<usize> {
        todo!()
    }

    fn remove(&mut self, key: usize) -> bool {
        todo!()
    }

    fn split_out(&mut self) -> (usize, Self) {
        todo!()
    }

    fn new_internal() -> Self {
        todo!()
    }

    fn init_as_root(&mut self, key: usize, i1: usize, i2: usize) {
        todo!()
    }
}

impl BTree<Data, Page> for File {
    fn node_ref(&self, i: usize) -> &Page {
        // self.pager.get_ref(i)
        todo!()
    }

    fn node_mut(&mut self, i: usize) -> &mut Page {
        todo!()
    }

    fn push(&mut self, node: Page) -> usize {
        todo!()
    }

    fn swap(&mut self, i: usize, node: Page) -> Page {
        todo!()
    }
}
