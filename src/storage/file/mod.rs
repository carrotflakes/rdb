mod impl_btree;
mod pager;

use std::borrow::Borrow;

use crate::{btree::{BTree}, data::{Data, Type, data_vec_from_bytes, data_vec_to_bytes}, schema::Schema, storage::Storage};

use self::{
    impl_btree::{Meta, BTreeCursor},
    pager::{PageRaw, Pager},
};

pub struct File {
    pager: Pager<Page>,
    schema: Schema,
    source_page_indices: Vec<usize>,
    key_types: Vec<Vec<Type>>,
    auto_increment: u64,
}

pub struct FileCursor {
    source_index: usize,
    meta: Meta,
    btree_cursor: BTreeCursor,
}

impl Storage for File {
    type Cursor = FileCursor;
    type SourceIndex = usize;

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn add_table(&mut self, table: crate::schema::Table) {
        let page = &mut self.pager.get_mut(0)[..];
        dbg!(bincode::serialize_into(page, &self.schema)).unwrap(); // todo: over size

        let page_index = self.add_root_node();
        self.source_page_indices.push(page_index); // ??
        self.key_types.push(if let Some(primary_key) = table.primary_key {
            vec![table.columns[primary_key].dtype.clone()]
        } else {
            vec![]
        });
        
        self.schema.tables.push(table);
    }

    fn issue_auto_increment(&mut self, table_name: &str, column_name: &str) -> u64 {
        self.auto_increment += 1;
        self.auto_increment
    }

    fn source_index(&self, table_name: &str, key_columns: &[String]) -> Option<Self::SourceIndex> {
        let (i, _table) = self.schema.get_table(table_name)?;
        Some(self.source_page_indices[i])
        // todo!()
    }

    fn get_cursor_first(&self, source_index: Self::SourceIndex) -> Self::Cursor {
        let meta = Meta {
            key_size: None,
            value_size: None,
        };
        FileCursor {
            source_index,
            meta: Meta {
                key_size: None,
                value_size: None,
            },
            btree_cursor: self.first_cursor(&meta, source_index),
        }
    }

    fn get_cursor_just(&self, source_index: Self::SourceIndex, key: &Vec<Data>) -> Self::Cursor {
        // let index;
        // self.find(source_index, key)
        // FileCursor {
        //     source_index,
        //     index,
        //     end: false,
        // }
        todo!()
    }

    fn cursor_get_row(&self, cursor: &Self::Cursor) -> Option<Vec<Data>> {
        self.cursor_get(&cursor.meta, &cursor.btree_cursor)
            .map(|x| data_vec_from_bytes(&self.key_types[cursor.source_index], &x.1).unwrap())
    }

    fn cursor_advance(&self, cursor: &mut Self::Cursor) -> bool {
        let c = self.cursor_next(&cursor.meta, &cursor.btree_cursor);
        cursor.btree_cursor = c;
        true
    }

    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool {
        <Self as BTree<Vec<u8>, Vec<u8>>>::cursor_is_end(
            &self,
            &cursor.meta,
            &cursor.btree_cursor,
        )
    }

    fn cursor_delete(&self, cursor: &mut Self::Cursor) -> bool {
        todo!()
    }

    fn cursor_update(&self, cursor: &mut Self::Cursor, data: Vec<Data>) -> bool {
        todo!()
    }

    fn add_row(&mut self, table_name: &str, data: Vec<Data>) -> Result<(), String> {
        let (i, table) = self.schema.get_table(table_name).unwrap();
        let key_size = if let Some(primary_key) = table.primary_key {
            table.columns[primary_key].dtype.size()
        } else {
            Some(0)
        };
        let value_size = table
            .columns
            .iter()
            .map(|c| c.dtype.size())
            .collect::<Option<Vec<usize>>>()
            .map(|ss| ss.into_iter().sum::<usize>());
        let meta = Meta {
            key_size,
            value_size,
        };
        let node_i = self.source_page_indices[i];
        let key = if let Some(primary_key) = table.primary_key {
            vec![data[primary_key].clone()]
        } else {
            vec![]
        };
        let key = data_vec_to_bytes(&key);
        let value = data_vec_to_bytes(&data);
        self.insert(&meta, node_i, &key, &value)
    }
}

impl File {
    pub fn open(filepath: &str) -> Self {
        let mut pager = Pager::<Page>::open(filepath);
        if pager.size() == 0 {
            // initialize
            pager.get_ref(0);
            Self {
                pager,
                schema: Schema::new_empty(),
                source_page_indices: vec![],
                key_types: vec![],
                auto_increment: 1000,
            }
        } else {
            let first_page: &Page = pager.get_ref(0);
            let schema = bincode::deserialize(&first_page.borrow()[..]).unwrap();
            dbg!(&schema);
            Self {
                pager,
                schema,
                source_page_indices: vec![],
                key_types: vec![],
                auto_increment: 1000,
            }
        }
    }
}

pub struct Page {
    raw: PageRaw,
}

impl From<PageRaw> for Page {
    fn from(raw: PageRaw) -> Self {
        Page { raw }
    }
}

impl std::ops::Deref for Page {
    type Target = PageRaw;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl std::ops::DerefMut for Page {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.raw
    }
}

#[test]
fn test() {
    let mut f = File::open("hello");
    f.add_table(crate::schema::Table {
        name: "hey".to_owned(),
        columns: vec![],
        primary_key: Some(0),
        constraints: vec![],
    });

    f.pager.save();
}
