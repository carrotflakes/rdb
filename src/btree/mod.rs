mod test;

#[derive(Debug, Clone)]
pub struct BTreeCursor {
    node_i: usize,
    value_i: usize,
}

pub trait BTreeNode<K: Clone + PartialEq + PartialOrd, V: Clone> {
    type Meta;

    fn is_leaf(&self, meta: &Self::Meta) -> bool;
    fn get_parent(&self, meta: &Self::Meta) -> Option<usize>;
    fn set_parent(&mut self, meta: &Self::Meta, i: usize);
    // returns number of values (not keys)
    fn size(&self, meta: &Self::Meta) -> usize;
    fn split_out(&mut self, meta: &Self::Meta) -> (K, Self);

    fn insert_node(&mut self, meta: &Self::Meta, key: &K, node_i: usize) -> bool;
    fn get_child(&self, meta: &Self::Meta, key: &K) -> usize;
    fn get_first_child(&self, meta: &Self::Meta) -> usize;
    fn get_children(&self, meta: &Self::Meta) -> Vec<usize>;
    fn new_internal(meta: &Self::Meta) -> Self;
    fn init_as_root_internal(&mut self, meta: &Self::Meta, key: &K, i1: usize, i2: usize);

    fn insert_value(&mut self, meta: &Self::Meta, key: &K, value: &V) -> bool;
    fn get_next(&self, meta: &Self::Meta) -> Option<usize>;
    fn set_next(&mut self, meta: &Self::Meta, i: usize);

    fn first_cursor(&self, meta: &Self::Meta) -> usize;
    fn find(&self, meta: &Self::Meta, key: &K) -> Option<usize>;
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
    ) -> Option<BTreeCursor> {
        let node = self.node_ref(node_i);
        if node.is_leaf(meta) {
            Some(BTreeCursor {
                node_i,
                value_i: node.find(meta, key)?,
            })
        } else {
            let node_i = node.get_child(meta, key);
            self.find(meta, node_i, key)
        }
    }

    fn insert(
        &mut self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        node_i: usize,
        key: &K,
        value: &V,
    ) -> Result<(), String> {
        // let node = self.node_mut(node_i);
        if self.node_ref(node_i).is_leaf(meta) {
            if self.node_mut(node_i).insert_value(meta, key, value) {
                Ok(())
            } else {
                let (pivot_key, mut new_node) = self.node_mut(node_i).split_out(meta);
                if key < &pivot_key {
                    self.node_mut(node_i).insert_value(meta, key, value);
                } else {
                    new_node.insert_value(meta, key, value);
                }
                self.insert_node(meta, node_i, &pivot_key, new_node)?;
                Ok(())
            }
        } else {
            let node_i = self.node_ref(node_i).get_child(meta, key);
            debug_assert_ne!(node_i, 0);
            self.insert(meta, node_i, key, value)
        }
    }

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
                // 親ノードがいっぱいなので分割する
                let (pivot_key, mut new_node) = self.node_mut(parent_i).split_out(meta);
                if key < &pivot_key {
                    self.node_mut(parent_i)
                        .insert_node(meta, key, inserted_node_i);
                    self.node_mut(inserted_node_i).set_parent(meta, parent_i);
                    let i = self.insert_node(meta, parent_i, &pivot_key, new_node)?;
                    self.reparent(meta, i);
                } else {
                    new_node.insert_node(meta, key, inserted_node_i);
                    let parent_i = self.insert_node(meta, parent_i, &pivot_key, new_node)?;
                    self.node_mut(inserted_node_i).set_parent(meta, parent_i);
                    self.reparent(meta, parent_i);
                }
            }
            self.node_mut(node_i).set_next(meta, inserted_node_i);
            Ok(inserted_node_i)
        } else {
            // ルートノードのとき
            let n = self.swap(node_i, Self::Node::new_internal(meta));
            let i1 = self.push(n);
            let i2 = self.push(node);
            {
                let node_i1 = self.node_mut(i1);
                node_i1.set_parent(meta, node_i);
                node_i1.set_next(meta, i2);
            }
            self.node_mut(i2).set_parent(meta, node_i);
            self.node_mut(node_i)
                .init_as_root_internal(meta, key, i1, i2);
            self.reparent(meta, i1);
            Ok(i2)
        }
    }

    fn reparent(&mut self, meta: &<Self::Node as BTreeNode<K, V>>::Meta, node_i: usize) {
        if !self.node_ref(node_i).is_leaf(meta) {
            for child_i in self.node_ref(node_i).get_children(meta) {
                self.node_mut(child_i).set_parent(meta, node_i);
            }
        }
    }

    fn cursor_get(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: &BTreeCursor,
    ) -> Option<(K, V)> {
        let BTreeCursor {
            mut node_i,
            mut value_i,
        } = cursor.clone();
        // NOTE: 現在地がpage.size()を超えている可能性があるのでnext pageを探る
        let mut node = self.node_ref(node_i);
        while node.size(meta) <= value_i {
            value_i = 0;
            if let Some(next_node_i) = node.get_next(meta) {
                node_i = next_node_i;
            } else {
                return None;
            }
            node = self.node_ref(node_i);
        }

        node.cursor_get(meta, cursor.value_i)
    }

    fn cursor_delete(
        &mut self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: &BTreeCursor,
    ) -> Option<BTreeCursor> {
        let mut cursor = cursor.clone();
        while self.node_ref(cursor.node_i).size(meta) <= cursor.value_i {
            cursor = self.cursor_next(meta, cursor);
        }
        if !self
            .node_mut(cursor.node_i)
            .cursor_delete(meta, cursor.value_i)
        {
            panic!("something went wrong :(")
        }
        Some(cursor)
    }

    fn cursor_next(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: BTreeCursor,
    ) -> BTreeCursor {
        let BTreeCursor {
            mut node_i,
            mut value_i,
        } = cursor.clone();
        let mut node = self.node_ref(node_i);
        value_i += 1;
        while node.size(meta) <= value_i {
            value_i = 0;
            if let Some(next_node_i) = node.get_next(meta) {
                node_i = next_node_i;
            } else {
                node_i = 0;
                break;
            }
            node = self.node_ref(node_i);
        }
        BTreeCursor { node_i, value_i }
    }

    fn cursor_is_end(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: &BTreeCursor,
    ) -> bool {
        cursor.node_i == 0 || {
            // TODO
            let node = self.node_ref(cursor.node_i);
            node.size(meta) <= cursor.value_i
                && if let Some(next) = node.get_next(meta) {
                    self.cursor_is_end(
                        meta,
                        &BTreeCursor {
                            node_i: next,
                            value_i: 0,
                        },
                    )
                } else {
                    true
                }
        }
    }

    fn cursor_next_occupied(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        mut cursor: BTreeCursor,
    ) -> BTreeCursor {
        let mut node = self.node_ref(cursor.node_i);
        while node.size(meta) <= cursor.value_i {
            if let Some(next_node_i) = node.get_next(meta) {
                cursor = BTreeCursor {
                    node_i: next_node_i,
                    value_i: 0,
                };
                node = self.node_ref(next_node_i);
            } else {
                break;
            }
        }
        cursor
    }
}
