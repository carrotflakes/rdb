use std::sync::{RwLockReadGuard, RwLockWriteGuard};

// #[cfg(test)]
// mod test;

#[derive(Debug, Clone)]
pub struct BTreeCursor {
    node_i: usize,
    value_i: usize,
}

pub trait BTreeNode<K: Clone + PartialEq + PartialOrd, V: Clone> {
    type Meta;

    // common

    fn is_leaf(&self, meta: &Self::Meta) -> bool;
    fn get_parent(&self, meta: &Self::Meta) -> Option<usize>;
    fn set_parent(&mut self, meta: &Self::Meta, i: usize);
    // returns number of values (not keys)
    fn size(&self, meta: &Self::Meta) -> usize;
    fn split_out(&mut self, meta: &Self::Meta) -> (K, Self);

    // internal nodes only

    fn insert_node(&mut self, meta: &Self::Meta, key: &K, node_i: usize) -> bool;
    fn get_child(&self, meta: &Self::Meta, key: &K) -> usize;
    fn get_first_child(&self, meta: &Self::Meta) -> usize;
    fn get_children(&self, meta: &Self::Meta) -> Vec<usize>;
    fn new_internal(meta: &Self::Meta) -> Self;
    fn init_as_root_internal(&mut self, meta: &Self::Meta, key: &K, i1: usize, i2: usize);

    // leaf nodes only

    fn insert_value(&mut self, meta: &Self::Meta, key: &K, value: &V) -> bool;
    fn get_next(&self, meta: &Self::Meta) -> Option<usize>;
    fn set_next(&mut self, meta: &Self::Meta, i: usize);
    fn find_cursor(&self, meta: &Self::Meta, key: &K) -> (usize, bool);
    fn first_cursor(&self, meta: &Self::Meta) -> usize;
    fn cursor_get(&self, meta: &Self::Meta, cursor: usize) -> Option<(K, V)>;
    fn cursor_delete(&mut self, meta: &Self::Meta, cursor: usize) -> bool;
}

pub trait NodeRef<T>: Sized {
    type R: std::ops::Deref<Target = T>;
    type W: std::ops::DerefMut<Target = T>;

    fn read(&self) -> RwLockReadGuard<Self::R>;
    fn write(&self) -> RwLockWriteGuard<Self::W>;
}

pub trait BTree<K: Clone + PartialEq + PartialOrd, V: Clone> {
    type Node: BTreeNode<K, V>;
    type NodeRef: NodeRef<Self::Node>;

    fn add_root_node(&mut self) -> usize;
    fn get_node(&self, node_i: usize) -> Self::NodeRef;
    fn push(&mut self, node: Self::Node) -> usize;
    fn swap(&mut self, node_i: usize, node: Self::Node) -> Self::Node;

    fn first_cursor(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        node_i: usize,
    ) -> BTreeCursor {
        let node = self.get_node(node_i);
        let node = node.read();
        if node.is_leaf(meta) {
            BTreeCursor {
                node_i,
                value_i: node.first_cursor(meta),
            }
        } else {
            self.first_cursor(meta, node.get_first_child(meta))
        }
    }

    fn find(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        node_i: usize,
        key: &K,
    ) -> (BTreeCursor, bool) {
        debug_assert_ne!(node_i, 0);
        let node_ref = self.get_node(node_i);
        let node = node_ref.read();
        if node.is_leaf(meta) {
            let mut node_i = node_i;
            std::mem::drop(node);
            std::mem::drop(node_ref);
            loop {
                let node_ref = self.get_node(node_i);
                let node = node_ref.read();
                let r = node.find_cursor(meta, key);
                if node.size(meta) > r.0 {
                    return (
                        BTreeCursor {
                            node_i,
                            value_i: r.0,
                        },
                        r.1,
                    );
                }
                if let Some(next_node_i) = node.get_next(meta) {
                    node_i = next_node_i;
                    if node_i != 0 {
                        continue;
                    }
                }
                return (
                    BTreeCursor {
                        node_i: 0,
                        value_i: 0,
                    },
                    false,
                );
            }
        } else {
            let child_node_i = node.get_child(meta, key);
            if child_node_i == 0 {
                dbg!(node.get_children(meta));
            }
            self.find(meta, child_node_i, key)
        }
    }

    fn insert(
        &mut self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        node_i: usize,
        key: &K,
        value: &V,
    ) -> Result<(), String> {
        debug_assert_ne!(node_i, 0);
        if self.get_node(node_i).read().is_leaf(meta) {
            if self.get_node(node_i).write().insert_value(meta, key, value) {
                Ok(())
            } else {
                let next_i = self.get_node(node_i).read().get_next(meta);
                let (pivot_key, mut new_node) = self.get_node(node_i).write().split_out(meta);
                if key < &pivot_key {
                    self.get_node(node_i).write().insert_value(meta, key, value);
                } else {
                    new_node.insert_value(meta, key, value);
                }
                let new_node_i = self.insert_node(meta, node_i, &pivot_key, new_node)?;
                let node_ref = self.get_node(new_node_i);
                let mut node = node_ref.write();
                debug_assert!(node.is_leaf(meta));
                if next_i.is_some() {
                    node.set_next(meta, next_i.unwrap());
                }
                Ok(())
            }
        } else {
            let child_node_i = self.get_node(node_i).read().get_child(meta, key);
            if child_node_i == 0 {
                dbg!(self.get_node(node_i).read().get_children(meta));
            }
            self.insert(meta, child_node_i, key, value)
        }
    }

    // nodeをnode_iのnextのノードとして追加する
    fn insert_node(
        &mut self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        node_i: usize,
        key: &K,
        node: Self::Node,
    ) -> Result<usize, String> {
        if let Some(parent_i) = self.get_node(node_i).read().get_parent(meta) {
            let inserted_node_i = self.push(node);
            if self
                .get_node(parent_i)
                .write()
                .insert_node(meta, key, inserted_node_i)
            {
                self.get_node(inserted_node_i)
                    .write()
                    .set_parent(meta, parent_i);
            } else {
                // 親ノードがいっぱいなので分割する
                let (pivot_key, mut new_node) = self.get_node(parent_i).write().split_out(meta);
                if key < &pivot_key {
                    self.get_node(parent_i)
                        .write()
                        .insert_node(meta, key, inserted_node_i);
                    self.get_node(inserted_node_i)
                        .write()
                        .set_parent(meta, parent_i);
                    let parent2_i = self.insert_node(meta, parent_i, &pivot_key, new_node)?;
                    self.reparent(meta, parent2_i);
                } else {
                    new_node.insert_node(meta, key, inserted_node_i);
                    let parent2_i = self.insert_node(meta, parent_i, &pivot_key, new_node)?;
                    self.get_node(inserted_node_i)
                        .write()
                        .set_parent(meta, parent2_i);
                    self.reparent(meta, parent2_i);
                }
            }
            let node_ref = self.get_node(node_i);
            let mut node = node_ref.write();
            if node.is_leaf(meta) {
                // is_leaf チェック必要?
                node.set_next(meta, inserted_node_i);
            }
            Ok(inserted_node_i)
        } else {
            // ルートノードのとき
            let n = self.swap(node_i, Self::Node::new_internal(meta));
            let node_i1 = self.push(n);
            let node_i2 = self.push(node);
            {
                let node_ref = self.get_node(node_i1);
                let mut node1 = node_ref.write();
                node1.set_parent(meta, node_i);
                if node1.is_leaf(meta) {
                    node1.set_next(meta, node_i2);
                }
            }
            self.get_node(node_i2).write().set_parent(meta, node_i);
            self.get_node(node_i)
                .write()
                .init_as_root_internal(meta, key, node_i1, node_i2);
            self.reparent(meta, node_i1);
            Ok(node_i2)
        }
    }

    fn reparent(&mut self, meta: &<Self::Node as BTreeNode<K, V>>::Meta, node_i: usize) {
        if !self.get_node(node_i).read().is_leaf(meta) {
            for child_node_i in self.get_node(node_i).read().get_children(meta) {
                self.get_node(child_node_i).write().set_parent(meta, node_i);
            }
        }
    }

    fn cursor_get(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: &BTreeCursor,
    ) -> Option<(K, V)> {
        let cursor = self.cursor_next_occupied(meta, cursor.clone());
        self.get_node(cursor.node_i)
            .read()
            .cursor_get(meta, cursor.value_i)
    }

    fn cursor_delete(
        &mut self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: BTreeCursor,
    ) -> BTreeCursor {
        let cursor = self.cursor_next_occupied(meta, cursor);
        if !self
            .get_node(cursor.node_i)
            .write()
            .cursor_delete(meta, cursor.value_i)
        {
            panic!("something went wrong :(")
        }
        cursor
    }

    fn cursor_next(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        mut cursor: BTreeCursor,
    ) -> BTreeCursor {
        cursor.value_i += 1;
        self.cursor_next_occupied(meta, cursor)
    }

    fn cursor_next_occupied(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        mut cursor: BTreeCursor,
    ) -> BTreeCursor {
        if cursor.node_i == 0 {
            return cursor;
        }

        let node_ref = self.get_node(cursor.node_i);
        let node = node_ref.read();
        if node.size(meta) <= cursor.value_i {
            if let Some(next_node_i) = node.get_next(meta) {
                cursor.node_i = next_node_i;
                loop {
                    let node_ref = self.get_node(cursor.node_i);
                    let node = node_ref.read();
                    cursor.value_i = node.first_cursor(meta);
                    if node.size(meta) <= cursor.value_i {
                        if let Some(next_node_i) = node.get_next(meta) {
                            cursor.node_i = next_node_i;
                        } else {
                            cursor.node_i = 0;
                            break;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        cursor
    }

    fn cursor_is_end(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: &BTreeCursor,
    ) -> bool {
        self.cursor_next_occupied(meta, cursor.clone()).node_i == 0
    }
}
