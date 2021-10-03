#![allow(warnings)]

use super::{BTree, BTreeNode};

#[derive(Debug)]
pub struct IBTreeNode<V> {
    parent: Option<usize>,
    keys: Vec<usize>,
    values: Result<Vec<usize>, Vec<V>>,
}

#[derive(Debug)]
pub struct IBTree {
    pages: Vec<IBTreeNode<String>>,
}

impl<V: Clone> BTreeNode<usize, V> for IBTreeNode<V> {
    type Meta = ();
    type Cursor = usize;

    fn is_leaf(&self, _: &()) -> bool {
        self.values.is_err()
    }

    fn get_parent(&self, _: &()) -> Option<usize> {
        self.parent.clone()
    }

    fn set_parent(&mut self, _: &(), i: usize) {
        self.parent = Some(i)
    }

    fn size(&self, _: &()) -> usize {
        match &self.values {
            Ok(x) => x.len(),
            Err(x) => x.len(),
        }
    }

    fn is_full(&self, _: &()) -> bool {
        match &self.values {
            Ok(x) => x.len() == 4,
            Err(x) => x.len() == 3,
        }
    }

    fn insert(&mut self, meta: &(), key: &usize, value: &V) -> bool {
        if self.is_full(meta) {
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

    fn insert_node(&mut self, _: &(), key: &usize, node_i: usize) {
        if let Ok(children) = &mut self.values {
            for i in 0..self.keys.len() {
                if &self.keys[i] == key {
                    panic!("dup");
                } else if key < &self.keys[i] {
                    self.keys.insert(i, key.clone());
                    children.insert(i + 1, node_i);
                    return;
                }
            }
            self.keys.push(key.clone());
            children.push(node_i);
        } else {
            panic!("b")
        }
    }

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

    fn remove(&mut self, _: &(), key: &usize) -> bool {
        if let Some(i) = self.keys.iter().position(|k| k == key) {
            if let Err(values) = &mut self.values {
                self.keys.remove(i);
                values.remove(i);
                true
            } else {
                panic!("ook");
            }
        } else {
            false
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
                        values: Err(vs.drain(2..).collect()),
                    },
                )
            }
        }
    }

    fn new_internal(_: &()) -> Self {
        IBTreeNode {
            parent: None,
            keys: vec![],
            values: Ok(vec![]),
        }
    }

    fn init_as_root_internal(&mut self, _: &(), key: &usize, i1: usize, i2: usize) {
        self.keys = vec![key.clone()];
        self.values = Ok(vec![i1, i2]);
    }

    fn first_cursor(&self, meta: &Self::Meta) -> Self::Cursor {
        0
    }

    fn find(&self, meta: &Self::Meta, key: &usize) -> Option<Self::Cursor> {
        if let Some(i) = self.keys.iter().position(|k| k == key) {
            if let Err(values) = &self.values {
                Some(i)
            } else {
                panic!("ook");
            }
        } else {
            None
        }
    }

    fn cursor_get(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> Option<(usize, V)> {
        if let Err(values) = &self.values {
            Some((self.keys[*cursor].clone(), values[*cursor].clone()))
        } else {
            panic!("ook");
        }
    }

    fn cursor_next(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> Self::Cursor {
        todo!()
    }

    fn cursor_is_end(&self, meta: &Self::Meta, cursor: &Self::Cursor) -> bool {
        todo!()
    }
}

impl BTree<usize, String> for IBTree {
    type Node = IBTreeNode<String>;

    fn add_root_node(&mut self) -> usize {
        self.pages.push(IBTreeNode {
            parent: None,
            keys: vec![],
            values: Err(vec![]),
        });
        self.pages.len() - 1
    }

    fn node_ref(&self, i: usize) -> &IBTreeNode<String> {
        &self.pages[i]
    }

    fn node_mut(&mut self, i: usize) -> &mut IBTreeNode<String> {
        &mut self.pages[i]
    }

    fn push(&mut self, node: IBTreeNode<String>) -> usize {
        self.pages.push(node);
        self.pages.len() - 1
    }

    fn swap(&mut self, i: usize, mut node: IBTreeNode<String>) -> IBTreeNode<String> {
        std::mem::swap(&mut node, &mut self.pages[i]);
        node
    }
}

impl IBTree {
    pub fn new() -> Self {
        IBTree {
            pages: vec![IBTreeNode {
                parent: None,
                keys: vec![],
                values: Err(vec![]),
            }],
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
        t.remove(&meta, 0, v);
    }
    t.show_nodes(0);
}
