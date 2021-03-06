#[cfg(test)]
mod test;

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

pub trait BTree<K: Clone + PartialEq + PartialOrd, V: Clone> {
    type Node: BTreeNode<K, V>;

    fn add_root_node(&mut self) -> usize;
    fn node_ref(&self, node_i: usize) -> &Self::Node;
    fn node_mut(&mut self, node_i: usize) -> &mut Self::Node;
    fn push(&mut self, node: Self::Node) -> usize;
    fn swap(&mut self, node_i: usize, node: Self::Node) -> Self::Node;

    fn first_cursor(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        node_i: usize,
    ) -> BTreeCursor {
        let node = self.node_ref(node_i);
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
        let mut node = self.node_ref(node_i);
        if node.is_leaf(meta) {
            let (mut i, mut found) = node.find_cursor(meta, key);
            let mut node_i = node_i;
            while node.size(meta) <= i {
                if let Some(next_node_i) = node.get_next(meta) {
                    node_i = next_node_i;
                    node = self.node_ref(node_i);
                    let r = node.find_cursor(meta, key);
                    i = r.0;
                    found = r.1;
                } else {
                    return (
                        BTreeCursor {
                            node_i: 0,
                            value_i: 0,
                        },
                        false,
                    );
                }
            }
            (BTreeCursor { node_i, value_i: i }, found)
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
        if self.node_ref(node_i).is_leaf(meta) {
            if self.node_mut(node_i).insert_value(meta, key, value) {
                Ok(())
            } else {
                let next_i = self.node_ref(node_i).get_next(meta);
                let (pivot_key, mut new_node) = self.node_mut(node_i).split_out(meta);
                if key < &pivot_key {
                    self.node_mut(node_i).insert_value(meta, key, value);
                } else {
                    new_node.insert_value(meta, key, value);
                }
                let new_node_i = self.insert_node(meta, node_i, &pivot_key, new_node)?;
                let node = self.node_mut(new_node_i);
                debug_assert!(node.is_leaf(meta));
                if next_i.is_some() {
                    node.set_next(meta, next_i.unwrap());
                }
                Ok(())
            }
        } else {
            let child_node_i = self.node_ref(node_i).get_child(meta, key);
            if child_node_i == 0 {
                dbg!(self.node_ref(node_i).get_children(meta));
            }
            self.insert(meta, child_node_i, key, value)
        }
    }

    // node???node_i???next?????????????????????????????????
    fn insert_node(
        &mut self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        node_i: usize,
        key: &K,
        node: Self::Node,
    ) -> Result<usize, String> {
        if let Some(parent_i) = self.node_ref(node_i).get_parent(meta) {
            let inserted_node_i = self.push(node);
            if self
                .node_mut(parent_i)
                .insert_node(meta, key, inserted_node_i)
            {
                self.node_mut(inserted_node_i).set_parent(meta, parent_i);
            } else {
                // ????????????????????????????????????????????????
                let (pivot_key, mut new_node) = self.node_mut(parent_i).split_out(meta);
                if key < &pivot_key {
                    self.node_mut(parent_i)
                        .insert_node(meta, key, inserted_node_i);
                    self.node_mut(inserted_node_i).set_parent(meta, parent_i);
                    let parent2_i = self.insert_node(meta, parent_i, &pivot_key, new_node)?;
                    self.reparent(meta, parent2_i);
                } else {
                    new_node.insert_node(meta, key, inserted_node_i);
                    let parent2_i = self.insert_node(meta, parent_i, &pivot_key, new_node)?;
                    self.node_mut(inserted_node_i).set_parent(meta, parent2_i);
                    self.reparent(meta, parent2_i);
                }
            }
            let node = self.node_mut(node_i);
            if node.is_leaf(meta) {
                // is_leaf ???????????????????
                node.set_next(meta, inserted_node_i);
            }
            Ok(inserted_node_i)
        } else {
            // ???????????????????????????
            let n = self.swap(node_i, Self::Node::new_internal(meta));
            let node_i1 = self.push(n);
            let node_i2 = self.push(node);
            {
                let node1 = self.node_mut(node_i1);
                node1.set_parent(meta, node_i);
                if node1.is_leaf(meta) {
                    node1.set_next(meta, node_i2);
                }
            }
            self.node_mut(node_i2).set_parent(meta, node_i);
            self.node_mut(node_i)
                .init_as_root_internal(meta, key, node_i1, node_i2);
            self.reparent(meta, node_i1);
            Ok(node_i2)
        }
    }

    fn reparent(&mut self, meta: &<Self::Node as BTreeNode<K, V>>::Meta, node_i: usize) {
        if !self.node_ref(node_i).is_leaf(meta) {
            for child_node_i in self.node_ref(node_i).get_children(meta) {
                self.node_mut(child_node_i).set_parent(meta, node_i);
            }
        }
    }

    fn cursor_get(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: &BTreeCursor,
    ) -> Option<(K, V)> {
        let cursor = self.cursor_next_occupied(meta, cursor.clone());
        let node = self.node_ref(cursor.node_i);
        node.cursor_get(meta, cursor.value_i)
    }

    fn cursor_delete(
        &mut self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: BTreeCursor,
    ) -> BTreeCursor {
        let cursor = self.cursor_next_occupied(meta, cursor);
        if !self
            .node_mut(cursor.node_i)
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
        let mut node = self.node_ref(cursor.node_i);
        while node.size(meta) <= cursor.value_i {
            if let Some(next_node_i) = node.get_next(meta) {
                cursor.node_i = next_node_i;
                node = self.node_ref(next_node_i);
                cursor.value_i = node.first_cursor(meta);
            } else {
                cursor.node_i = 0;
                break;
            }
        }
        cursor
    }

    fn cursor_is_end(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: &BTreeCursor,
    ) -> bool {
        if cursor.node_i == 0 {
            return true;
        }
        let mut cursor = cursor.clone();
        let mut node = self.node_ref(cursor.node_i);
        while node.size(meta) <= cursor.value_i {
            if let Some(next_node_i) = node.get_next(meta) {
                cursor.node_i = next_node_i;
                node = self.node_ref(cursor.node_i);
                cursor.value_i = node.first_cursor(meta);
            } else {
                return true;
            }
        }
        false
    }
}
