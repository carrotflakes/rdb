mod test;

pub type Key = usize;

pub trait BTreeNode<V: Clone> {
    type Meta;

    fn is_leaf(&self, meta: &Self::Meta) -> bool;
    fn get_parent(&self, meta: &Self::Meta) -> Option<usize>;
    fn set_parent(&mut self, meta: &Self::Meta, i: usize);
    fn size(&self, meta: &Self::Meta) -> usize;
    fn is_full(&self, meta: &Self::Meta) -> bool;
    fn insert(&mut self, meta: &Self::Meta, key: Key, value: &V) -> bool;
    fn insert_node(&mut self, meta: &Self::Meta, key: Key, node_i: usize);
    fn get(&self, meta: &Self::Meta, key: Key) -> Option<V>;
    fn get_child(&self, meta: &Self::Meta, key: Key) -> usize;
    fn get_children(&self, meta: &Self::Meta) -> Vec<usize>;
    fn remove(&mut self, meta: &Self::Meta, key: Key) -> bool;
    fn split_out(&mut self, meta: &Self::Meta) -> (usize, Self);
    fn new_internal(meta: &Self::Meta) -> Self;
    fn init_as_root(&mut self, meta: &Self::Meta, key: Key, i1: usize, i2: usize);
}

pub trait BTree<V: Clone> {
    type Node: BTreeNode<V>;

    fn add_root_node(&mut self) -> usize;
    fn node_ref(&self, node_i: usize) -> &Self::Node;
    fn node_mut(&mut self, node_i: usize) -> &mut Self::Node;
    fn push(&mut self, node: Self::Node) -> usize;
    fn swap(&mut self, node_i: usize, node: Self::Node) -> Self::Node;

    fn find(
        &self,
        meta: &<<Self as BTree<V>>::Node as BTreeNode<V>>::Meta,
        node_i: usize,
        key: Key,
    ) -> Option<V> {
        let node = self.node_ref(node_i);
        if node.is_leaf(meta) {
            node.get(meta, key)
        } else {
            let node_i = node.get_child(meta, key);
            self.find(meta, node_i, key)
        }
    }

    fn insert(
        &mut self,
        meta: &<<Self as BTree<V>>::Node as BTreeNode<V>>::Meta,
        node_i: usize,
        key: Key,
        value: &V,
    ) -> Result<(), String> {
        let node = self.node_mut(node_i);
        if node.is_leaf(meta) {
            if node.insert(meta, key, value) {
                Ok(())
            } else {
                let (pivot_key, mut new_node) = node.split_out(meta);
                if key < pivot_key {
                    node.insert(meta, key, value);
                } else {
                    new_node.insert(meta, key, value);
                }
                self.insert_node(meta, node_i, pivot_key, new_node)
                    .map(|_| ())
            }
        } else {
            let node_i = node.get_child(meta, key);
            self.insert(meta, node_i, key, value)
        }
    }

    fn insert_node(
        &mut self,
        meta: &<<Self as BTree<V>>::Node as BTreeNode<V>>::Meta,
        node_i: usize,
        key: Key,
        node: Self::Node,
    ) -> Result<usize, String> {
        if let Some(parent_i) = self.node_ref(node_i).get_parent(meta) {
            let inserted_node_i = self.push(node);
            if self.node_ref(parent_i).is_full(meta) {
                // 親ノードがいっぱいなので分割する
                let (pivot_key, mut new_node) = self.node_mut(parent_i).split_out(meta);
                if key < pivot_key {
                    self.node_mut(parent_i)
                        .insert_node(meta, key, inserted_node_i);
                    self.node_mut(inserted_node_i).set_parent(meta, parent_i);
                    let i = self.insert_node(meta, parent_i, pivot_key, new_node)?;
                    self.reparent(meta, i);
                } else {
                    new_node.insert_node(meta, key, inserted_node_i);
                    let parent_i = self.insert_node(meta, parent_i, pivot_key, new_node)?;
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
            self.node_mut(node_i).init_as_root(meta, key, i1, i2);
            self.reparent(meta, i1);
            Ok(i2)
        }
    }

    fn reparent(&mut self, meta: &<<Self as BTree<V>>::Node as BTreeNode<V>>::Meta, node_i: usize) {
        if !self.node_ref(node_i).is_leaf(meta) {
            for child_i in self.node_ref(node_i).get_children(meta) {
                self.node_mut(child_i).set_parent(meta, node_i);
            }
        }
    }

    fn remove(
        &mut self,
        meta: &<<Self as BTree<V>>::Node as BTreeNode<V>>::Meta,
        node_i: usize,
        key: Key,
    ) -> bool {
        let node = self.node_mut(node_i);
        if node.is_leaf(meta) {
            node.remove(meta, key)
        } else {
            let node_i = node.get_child(meta, key);
            self.remove(meta, node_i, key)
        }
    }
}
