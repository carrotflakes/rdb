mod test;

pub trait BTreeNode<V: Clone> {
    fn is_leaf(&self) -> bool;
    fn get_parent(&self) -> Option<usize>;
    fn set_parent(&mut self, i: usize);
    fn size(&self) -> usize;
    fn is_full(&self) -> bool;
    fn insert(&mut self, key: usize, value: V);
    fn insert_node(&mut self, key: usize, node_i: usize);
    fn get(&self, key: usize) -> Option<V>;
    fn get_child(&self, key: usize) -> usize;
    fn get_children(&self) -> Vec<usize>;
    fn remove(&mut self, key: usize) -> bool;
    fn split_out(&mut self) -> (usize, Self);
    fn new_internal() -> Self;
    fn init_as_root(&mut self, key: usize, i1: usize, i2: usize);
}

pub trait BTree<V: Clone, N: BTreeNode<V>> {
    fn node_ref(&self, i: usize) -> &N;
    fn node_mut(&mut self, i: usize) -> &mut N;
    fn push(&mut self, node: N) -> usize;
    fn swap(&mut self, i: usize, node: N) -> N;

    fn find(&self, node_i: usize, key: usize) -> Option<V> {
        let node = self.node_ref(node_i);
        if node.is_leaf() {
            node.get(key)
        } else {
            let node_i = node.get_child(key);
            self.find(node_i, key)
        }
    }

    fn insert(&mut self, node_i: usize, key: usize, value: V) -> Result<(), String> {
        let node = self.node_mut(node_i);
        if node.is_leaf() {
            if node.is_full() {
                let (pivot_key, mut new_node) = node.split_out();
                if key < pivot_key {
                    node.insert(key, value);
                } else {
                    new_node.insert(key, value);
                }
                self.insert_node(node_i, pivot_key, new_node).map(|_| ())
            } else {
                node.insert(key, value);
                Ok(())
            }
        } else {
            let node_i = node.get_child(key);
            self.insert(node_i, key, value)
        }
    }

    fn insert_node(&mut self, node_i: usize, key: usize, node: N) -> Result<usize, String> {
        if let Some(parent_i) = self.node_ref(node_i).get_parent() {
            let inserted_node_i = self.push(node);
            if self.node_ref(parent_i).is_full() {
                // 親ノードがいっぱいなので分割する
                let (pivot_key, mut new_node) = self.node_mut(parent_i).split_out();
                if key < pivot_key {
                    self.node_mut(parent_i).insert_node(key, inserted_node_i);
                    self.node_mut(inserted_node_i).set_parent(parent_i);
                    let i = self.insert_node(parent_i, pivot_key, new_node)?;
                    self.reparent(i);
                } else {
                    new_node.insert_node(key, inserted_node_i);
                    let parent_i = self.insert_node(parent_i, pivot_key, new_node)?;
                    self.node_mut(inserted_node_i).set_parent(parent_i);
                    self.reparent(parent_i);
                }
            } else {
                self.node_mut(parent_i).insert_node(key, inserted_node_i);
                self.node_mut(inserted_node_i).set_parent(parent_i);
            }
            Ok(inserted_node_i)
        } else {
            // ルートノードのとき
            let n = self.swap(node_i, N::new_internal());
            let i1 = self.push(n);
            let i2 = self.push(node);
            self.node_mut(i1).set_parent(node_i);
            self.node_mut(i2).set_parent(node_i);
            self.node_mut(node_i).init_as_root(key, i1, i2);
            self.reparent(i1);
            Ok(i2)
        }
    }

    fn reparent(&mut self, node_i: usize) {
        if !self.node_ref(node_i).is_leaf() {
            for child_i in self.node_ref(node_i).get_children() {
                self.node_mut(child_i).set_parent(node_i);
            }
        }
    }

    fn remove(&mut self, node_i: usize, key: usize) -> bool {
        let node = self.node_mut(node_i);
        if node.is_leaf() {
            node.remove(key)
        } else {
            let node_i = node.get_child(key);
            self.remove(node_i, key)
        }
    }
}
