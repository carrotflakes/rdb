mod test;

pub type BTreeCursor<K, V, B> =
    BTreeCursorInner<<<B as BTree<K, V>>::Node as BTreeNode<K, V>>::Cursor>;

pub struct BTreeCursorInner<NodeCursor: Clone> {
    node_i: usize,
    cursor: NodeCursor,
}

pub trait BTreeNode<K: Clone + PartialEq + PartialOrd, V: Clone> {
    type Meta;
    type Cursor: Clone;

    fn is_leaf(&self, meta: &Self::Meta) -> bool;
    fn get_parent(&self, meta: &Self::Meta) -> Option<usize>;
    fn set_parent(&mut self, meta: &Self::Meta, i: usize);
    // returns number of values (not keys)
    fn size(&self, meta: &Self::Meta) -> usize;
    // expect internal node
    fn is_full(&self, meta: &Self::Meta) -> bool;
    fn insert(&mut self, meta: &Self::Meta, key: &K, value: &V) -> bool;
    fn insert_node(&mut self, meta: &Self::Meta, key: &K, node_i: usize);
    fn get(&self, meta: &Self::Meta, key: &K) -> Option<V>;
    fn get_child(&self, meta: &Self::Meta, key: &K) -> usize;
    fn get_first_child(&self, meta: &Self::Meta) -> usize;
    fn get_children(&self, meta: &Self::Meta) -> Vec<usize>;
    fn remove(&mut self, meta: &Self::Meta, key: &K) -> bool;
    fn split_out(&mut self, meta: &Self::Meta) -> (K, Self);
    fn new_internal(meta: &Self::Meta) -> Self;
    fn init_as_root_internal(&mut self, meta: &Self::Meta, key: &K, i1: usize, i2: usize);

    fn first_cursor(&self, meta: &Self::Meta) -> Self::Cursor;
    fn find(&self, meta: &Self::Meta, key: &K) -> Option<Self::Cursor>;
    fn cursor_get(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> Option<(K, V)>;
    fn cursor_next(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> Self::Cursor;
    fn cursor_is_end(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> bool;
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
    ) -> BTreeCursorInner<<Self::Node as BTreeNode<K, V>>::Cursor> {
        let node = self.node_ref(node_i);
        if node.is_leaf(meta) {
            BTreeCursorInner {
                node_i,
                cursor: node.first_cursor(meta),
            }
        } else {
            self.first_cursor(meta, node.get_first_child(meta))
        }
    }

    fn find_one(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        node_i: usize,
        key: &K,
    ) -> Option<V> {
        let node = self.node_ref(node_i);
        if node.is_leaf(meta) {
            node.get(meta, key)
        } else {
            let node_i = node.get_child(meta, key);
            self.find_one(meta, node_i, key)
        }
    }

    fn find(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        node_i: usize,
        key: &K,
    ) -> Option<<Self::Node as BTreeNode<K, V>>::Cursor> {
        let node = self.node_ref(node_i);
        if node.is_leaf(meta) {
            node.find(meta, key)
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
        let node = self.node_mut(node_i);
        if node.is_leaf(meta) {
            if node.insert(meta, key, value) {
                Ok(())
            } else {
                let (pivot_key, mut new_node) = node.split_out(meta);
                if key < &pivot_key {
                    node.insert(meta, key, value);
                } else {
                    new_node.insert(meta, key, value);
                }
                self.insert_node(meta, node_i, &pivot_key, new_node)
                    .map(|_| ())
            }
        } else {
            let node_i = node.get_child(meta, key);
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
            if self.node_ref(parent_i).is_full(meta) {
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
            } else {
                self.node_mut(parent_i)
                    .insert_node(meta, key, inserted_node_i);
                self.node_mut(inserted_node_i).set_parent(meta, parent_i);
            }
            Ok(inserted_node_i)
        } else {
            // ルートノードのとき
            let n = self.swap(node_i, Self::Node::new_internal(meta));
            let i1 = self.push(n);
            let i2 = self.push(node);
            self.node_mut(i1).set_parent(meta, node_i);
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

    fn remove(
        &mut self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        node_i: usize,
        key: &K,
    ) -> bool {
        let node = self.node_mut(node_i);
        if node.is_leaf(meta) {
            node.remove(meta, key)
        } else {
            let node_i = node.get_child(meta, key);
            self.remove(meta, node_i, key)
        }
    }

    fn cursor_get(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: &BTreeCursor<K, V, Self>,
    ) -> Option<(K, V)> {
        self.node_ref(cursor.node_i)
            .cursor_get(meta, &cursor.cursor)
    }

    fn cursor_next(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: &BTreeCursor<K, V, Self>,
    ) -> BTreeCursor<K, V, Self> {
        BTreeCursorInner {
            node_i: cursor.node_i,
            cursor: self
                .node_ref(cursor.node_i)
                .cursor_next(meta, &cursor.cursor),
        }
    }

    fn cursor_is_end(
        &self,
        meta: &<Self::Node as BTreeNode<K, V>>::Meta,
        cursor: &BTreeCursor<K, V, Self>,
    ) -> bool {
        self.node_ref(cursor.node_i)
            .cursor_is_end(meta, &cursor.cursor)
    }
}
