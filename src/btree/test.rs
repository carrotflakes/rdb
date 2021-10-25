#![allow(warnings)]

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::{BTree, BTreeNode, NodeRef};

#[derive(Debug)]
pub struct IBTreeNode<V> {
    parent: Option<usize>,
    keys: Vec<usize>,
    next: Option<usize>,
    values: Result<Vec<usize>, Vec<V>>,
}

#[derive(Debug)]
pub struct IBTree {
    pages: Vec<Arc<RwLock<IBTreeNode<String>>>>,
}

impl<V: Clone> BTreeNode<usize, V> for IBTreeNode<V> {
    type Meta = ();

    fn is_leaf(&self, _: &()) -> bool {
        self.values.is_err()
    }

    fn get_parent(&self, _: &()) -> Option<usize> {
        self.parent.clone()
    }

    fn set_parent(&mut self, _: &(), i: usize) {
        self.parent = Some(i);
    }

    fn size(&self, _: &()) -> usize {
        match &self.values {
            Ok(x) => x.len(),
            Err(x) => x.len(),
        }
    }

    fn split_out(&mut self, _: &()) -> (usize, Self) {
        match &mut self.values {
            Ok(vs) => {
                let pivot = self.keys[1];
                (
                    pivot,
                    IBTreeNode {
                        parent: None,
                        keys: self.keys.drain(1..).skip(1).collect(),
                        next: None,
                        values: Ok(vs.drain(2..).collect()),
                    },
                )
            }
            Err(vs) => {
                let pivot = self.keys[2];
                (
                    pivot,
                    IBTreeNode {
                        parent: None,
                        keys: self.keys.drain(2..).collect(),
                        next: None,
                        values: Err(vs.drain(2..).collect()),
                    },
                )
            }
        }
    }

    fn insert_node(&mut self, _: &(), key: &usize, node_i: usize) -> bool {
        if let Ok(children) = &mut self.values {
            for i in 0..self.keys.len() {
                if &self.keys[i] == key {
                    panic!("dup");
                } else if key < &self.keys[i] {
                    self.keys.insert(i, key.clone());
                    children.insert(i + 1, node_i);
                    return true;
                }
            }
            self.keys.push(key.clone());
            children.push(node_i);
        } else {
            panic!("b")
        }
        true
    }

    fn get_child(&self, _: &(), key: &usize) -> usize {
        if let Ok(chuldren) = &self.values {
            for i in 0..self.keys.len() {
                if key < &self.keys[i] {
                    return chuldren[i];
                }
            }
            *chuldren.last().unwrap()
        } else {
            panic!("!")
        }
    }

    fn get_first_child(&self, meta: &Self::Meta) -> usize {
        if let Ok(chuldren) = &self.values {
            chuldren[0]
        } else {
            panic!("!")
        }
    }

    fn get_children(&self, _: &()) -> Vec<usize> {
        if let Ok(children) = &self.values {
            children.clone()
        } else {
            panic!("!!!")
        }
    }

    fn new_internal(_: &()) -> Self {
        IBTreeNode {
            parent: None,
            keys: vec![],
            next: None,
            values: Ok(vec![]),
        }
    }

    fn init_as_root_internal(&mut self, _: &(), key: &usize, i1: usize, i2: usize) {
        self.keys = vec![key.clone()];
        self.values = Ok(vec![i1, i2]);
    }

    fn insert_value(&mut self, meta: &(), key: &usize, value: &V) -> bool {
        if match &self.values {
            Ok(x) => x.len() == 4,
            Err(x) => x.len() == 3,
        } {
            return false;
        }
        if let Err(values) = &mut self.values {
            for i in 0..self.keys.len() {
                if &self.keys[i] == key {
                    panic!("dup");
                } else if key < &self.keys[i] {
                    self.keys.insert(i, key.clone());
                    values.insert(i, value.clone());
                    return true;
                }
            }
            self.keys.push(key.clone());
            values.push(value.clone());
            true
        } else {
            panic!("a")
        }
    }

    fn get_next(&self, _: &()) -> Option<usize> {
        self.next.clone()
    }

    fn set_next(&mut self, _: &(), i: usize) {
        self.next = Some(i);
    }

    fn find_cursor(&self, meta: &Self::Meta, key: &usize) -> (usize, bool) {
        if let Some(i) = self.keys.iter().position(|k| k >= key) {
            if self.values.is_err() {
                (i, self.keys[i] == *key)
            } else {
                panic!("ook");
            }
        } else {
            (self.keys.len(), false)
        }
    }

    fn first_cursor(&self, meta: &Self::Meta) -> usize {
        0
    }

    fn cursor_get(&self, meta: &Self::Meta, cursor: usize) -> Option<(usize, V)> {
        if let Err(values) = &self.values {
            Some((self.keys[cursor].clone(), values[cursor].clone()))
        } else {
            panic!("ook");
        }
    }

    fn cursor_delete(&mut self, meta: &Self::Meta, cursor: usize) -> bool {
        if let Err(values) = &mut self.values {
            self.keys.remove(cursor);
            values.remove(cursor);
            true
        } else {
            panic!("ook");
        }
    }
}

impl<V: Clone> NodeRef<IBTreeNode<V>> for Arc<RwLock<IBTreeNode<V>>>{
    fn read(&self) -> RwLockReadGuard<IBTreeNode<V>> {
        todo!()
    }

    fn write(&self) -> RwLockWriteGuard<IBTreeNode<V>> {
        todo!()
    }
}

impl BTree<usize, String> for IBTree {
    type Node = IBTreeNode<String>;
    type NodeRef = Arc<RwLock<IBTreeNode<String>>>;

    fn add_root_node(&mut self) -> usize {
        self.pages.push(Arc::new(RwLock::new(IBTreeNode {
            parent: None,
            keys: vec![],
            next: None,
            values: Err(vec![]),
        })));
        self.pages.len() - 1
    }

    fn get_node(&self, i: usize) -> Self::NodeRef {
        self.pages[i].clone()
    }

    fn push(&mut self, node: IBTreeNode<String>) -> usize {
        self.pages.push(Arc::new(RwLock::new(node)));
        self.pages.len() - 1
    }

    fn swap(&mut self, i: usize, mut node: IBTreeNode<String>) -> IBTreeNode<String> {
        std::mem::swap(&mut node, self.pages[i].get_mut().unwrap());
        node
    }
}

impl<V: Clone> IBTreeNode<V> {
    fn get(&self, _: &(), key: &usize) -> Option<V> {
        if let Some(i) = self.keys.iter().position(|k| k == key) {
            if let Err(values) = &self.values {
                Some(values[i].to_owned())
            } else {
                panic!("ook");
            }
        } else {
            None
        }
    }
}

impl IBTree {
    pub fn new() -> Self {
        IBTree {
            pages: vec![Arc::new(RwLock::new(IBTreeNode {
                parent: None,
                keys: vec![],
                next: None,
                values: Err(vec![]),
            }))],
        }
    }

    fn show_nodes(&self, i: usize) {
        self.show_node(i, "".to_string());
    }

    fn show_node(&self, i: usize, indent: String) {
        let page = &self.pages[i];
        println!("{}- {} {:?}", &indent, i, page.parent);

        let indent = indent + "  ";
        if page.is_leaf(&()) {
            println!("{}{:?}", &indent, &page.keys);
        } else {
            for i in 0..page.keys.len() {
                self.show_node(page.values.as_ref().ok().unwrap()[i], indent.clone());
                println!("{}{}", &indent, page.keys[i]);
            }
            self.show_node(*page.values.as_ref().ok().unwrap().last().unwrap(), indent);
        }
    }

    fn find_one(
        &self,
        meta: &<<IBTree as BTree<usize, String>>::Node as BTreeNode<usize, String>>::Meta,
        node_i: usize,
        key: &usize,
    ) -> Option<String> {
        let node = self.get_node(node_i);
        if node.is_leaf(meta) {
            node.get(meta, key)
        } else {
            let node_i = node.get_child(meta, key);
            self.find_one(meta, node_i, key)
        }
    }
}

#[test]
fn test() {
    #![allow(unused_must_use)]

    let mut set = IBTree::new();
    let meta = ();
    dbg!(set.find_one(&meta, 0, &10));
    dbg!(set.insert(&meta, 0, &10, &"10".to_string()));
    dbg!(set.find_one(&meta, 0, &10));
    dbg!(set.insert(&meta, 0, &20, &"20".to_string()));
    dbg!(set.insert(&meta, 0, &30, &"30".to_string()));
    dbg!(&set);
    dbg!(set.insert(&meta, 0, &40, &"40".to_string()));
    dbg!(&set);
    dbg!(set.find_one(&meta, 0, &20));
    dbg!(set.find_one(&meta, 0, &30));
    dbg!(set.insert(&meta, 0, &15, &"15".to_string()));
    dbg!(&set);
    dbg!(set.insert(&meta, 0, &16, &"16".to_string()));
    dbg!(&set);
    dbg!(set.insert(&meta, 0, &25, &"25".to_string()));
    dbg!(set.insert(&meta, 0, &45, &"45".to_string()));
    dbg!(&set);
    dbg!(set.find_one(&meta, 0, &10));
    dbg!(set.find_one(&meta, 0, &20));
    dbg!(set.find_one(&meta, 0, &30));
    dbg!(set.find_one(&meta, 0, &40));
    dbg!(set.find_one(&meta, 0, &15));

    let vs = vec![
        11, 5, 9, 2, 6, 8, 16, 20, 18, 3, 4, 10, 12, 15, 1, 14, 13, 19, 7, 17,
    ];
    // let vs = (1..=20).rev().collect::<Vec<usize>>();
    let mut t = IBTree::new();
    for v in &vs {
        t.insert(&meta, 0, v, &format!("{}", v)).unwrap();
        t.show_nodes(0);
        // dbg!(&t);
        println!();
    }
    for v in &vs {
        t.find_one(&meta, 0, v).unwrap();
    }
    for v in &vs {
        let c = t.find(&meta, 0, v).0;
        t.cursor_delete(&meta, c);
    }
    t.show_nodes(0);
}
