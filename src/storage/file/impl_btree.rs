use crate::{
    btree::{BTree, BTreeNode, Key},
    data::Data,
};

use super::{File, PageImpl, pager::{ PAGE_SIZE}};

pub struct Node {
    key_size: Option<usize>,
    value_size: Option<usize>,
    page: PageImpl,
}

// [1] leaf flag
// [4] parent node id
// [2] size

impl BTreeNode<Data> for PageImpl {
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
        use std::convert::TryInto;
        u16::from_le_bytes(self[1 + 4..1 + 4 + 2].as_ref().try_into().unwrap()) as usize
    }

    fn is_full(&self) -> bool {
        todo!()
    }

    fn insert(&mut self, key: Key, value: Data) {
        todo!()
    }

    fn insert_node(&mut self, key: Key, node_i: usize) {
        todo!()
    }

    fn get(&self, key: Key) -> Option<Data> {
        todo!()
    }

    fn get_child(&self, key: Key) -> usize {
        todo!()
    }

    fn get_children(&self) -> Vec<usize> {
        todo!()
    }

    fn remove(&mut self, key: Key) -> bool {
        todo!()
    }

    fn split_out(&mut self) -> (usize, Self) {
        todo!()
    }

    fn new_internal() -> Self {
        [0; PAGE_SIZE as usize].into()
    }

    fn init_as_root(&mut self, key: Key, i1: usize, i2: usize) {
        todo!()
    }
}

impl BTree<Data> for File {
    type Node = PageImpl;

    fn add_root_node(&mut self) -> usize {
        let page_i = self.pager.size();
        self.pager.get_ref(page_i);
        page_i
    }

    fn node_ref(&self, node_i: usize) -> &Self::Node {
        #[allow(mutable_transmutes)]
        unsafe { std::mem::transmute::<_, &mut super::pager::Pager<PageImpl>>(&self.pager) }.get_ref(node_i)
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
