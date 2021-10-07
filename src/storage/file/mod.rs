mod impl_btree;
mod pager;

use std::borrow::Borrow;

use crate::{
    btree::{BTree, BTreeCursor},
    data::{data_vec_from_bytes, data_vec_to_bytes, Data, Type},
    schema::Schema,
    storage::Storage,
};

use self::{
    impl_btree::Meta,
    pager::{PageRaw, Pager, PAGE_SIZE},
};

pub struct File {
    pager: Pager<Page>,
    schema: Schema,
    sources: Vec<Source>,
    auto_increment: u64,
}

pub struct Source {
    table_index: usize,
    page_index: usize,
    key_columns: Vec<String>,
    value_types: Vec<Type>,
    parent_source_index: Option<usize>,
    meta: Meta,
}

pub struct FileCursor {
    source_index: usize,
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
        if bincode::serialized_size(&self.schema).unwrap() <= page.len() as u64 {
            dbg!(bincode::serialize_into(page, &self.schema)).unwrap();
        } else {
            panic!("schema is too large")
        }

        let source_index = self.sources.len();
        let page_index = self.pager.add_root_node();
        self.pager.ensure_page(page_index); // FIXME
        self.sources.push(Source {
            table_index: self.schema.tables.len(),
            page_index,
            key_columns: if let Some(primary_key) = table.primary_key {
                vec![table.columns[primary_key].name.clone()]
            } else {
                vec![]
            },
            value_types: table.columns.iter().map(|c| c.dtype.clone()).collect(),
            parent_source_index: None,
            meta: Meta {
                key_size: if let Some(primary_key) = table.primary_key {
                    table.columns[primary_key].dtype.size()
                } else {
                    Some(0)
                },
                value_size: table
                    .columns
                    .iter()
                    .map(|c| c.dtype.size())
                    .collect::<Option<Vec<usize>>>()
                    .map(|ss| ss.into_iter().sum::<usize>()),
            },
        });

        // sources for indices
        for cols in table.indices.iter().map(|i| &i.column_indices) {
            let key_columns = cols
                .iter()
                .map(|ci| &table.columns[*ci])
                .collect::<Vec<_>>();
            let page_index = self.pager.add_root_node();
            self.pager.ensure_page(page_index); // FIXME
            self.sources.push(Source {
                table_index: self.schema.tables.len(),
                page_index,
                key_columns: key_columns.iter().map(|c| c.name.clone()).collect(),
                value_types: vec![Type::U64],
                parent_source_index: Some(source_index),
                meta: Meta {
                    key_size: key_columns.iter().map(|c| c.dtype.size()).sum(),
                    value_size: Type::U64.size(),
                },
            });
        }

        self.schema.tables.push(table);
    }

    fn issue_auto_increment(&mut self, table_name: &str, column_name: &str) -> u64 {
        self.auto_increment += 1;
        self.auto_increment
    }

    fn source_index(&self, table_name: &str, key_columns: &[String]) -> Option<Self::SourceIndex> {
        for (i, source) in self.sources.iter().enumerate() {
            let table = &self.schema.tables[source.table_index];
            if table.name == table_name && source.key_columns == key_columns {
                return Some(i);
            }
        }
        None
    }

    fn get_cursor_first(&self, source_index: Self::SourceIndex) -> Self::Cursor {
        let source = &self.sources[source_index];
        FileCursor {
            source_index,
            btree_cursor: self.pager.first_cursor(&source.meta, source.page_index),
        }
    }

    fn get_cursor_just(&self, source_index: Self::SourceIndex, key: &Vec<Data>) -> Self::Cursor {
        let source = &self.sources[source_index];
        let key = data_vec_to_bytes(key);
        FileCursor {
            source_index,
            btree_cursor: self
                .pager
                .find(&source.meta, source.page_index, &key)
                .unwrap(),
        }
    }

    fn cursor_get_row(&self, cursor: &Self::Cursor) -> Option<Vec<Data>> {
        let source = &self.sources[cursor.source_index];
        self.pager
            .cursor_get(&source.meta, &cursor.btree_cursor)
            .map(|x| data_vec_from_bytes(&source.value_types, &x.1).unwrap())
    }

    fn cursor_advance(&self, cursor: &mut Self::Cursor) -> bool {
        let source = &self.sources[cursor.source_index];
        let c = self.pager.cursor_next(&source.meta, &cursor.btree_cursor);
        cursor.btree_cursor = c;
        true
    }

    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool {
        let source = &self.sources[cursor.source_index];
        self.pager.cursor_is_end(&source.meta, &cursor.btree_cursor)
    }

    fn cursor_delete(&mut self, cursor: &mut Self::Cursor) -> bool {
        let source = &self.sources[cursor.source_index];
        if let Some(btree_cursor) = self.pager.cursor_delete(&source.meta, &cursor.btree_cursor) {
            cursor.btree_cursor = btree_cursor;
            true
        } else {
            false
        }
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
        let node_i = self.sources[i].page_index;
        let key = if let Some(primary_key) = table.primary_key {
            vec![data[primary_key].clone()]
        } else {
            vec![]
        };
        let key = data_vec_to_bytes(&key);
        let value = data_vec_to_bytes(&data);
        let r = self.pager.insert(&meta, node_i, &key, &value);
        r
    }

    fn flush(&self) {
        #[allow(mutable_transmutes)]
        let pager = unsafe { std::mem::transmute::<_, &mut Pager<Page>>(&self.pager) };
        pager.save()
    }
}

impl File {
    pub fn open(filepath: &str) -> Self {
        let mut pager = Pager::<Page>::open(filepath);
        if pager.size() == 0 {
            // initialize
            pager.get_mut(0);
            // TODO write file header
            // 0x1006 <storage format>
            Self {
                pager,
                schema: Schema::new_empty(),
                sources: vec![],
                auto_increment: 1000,
            }
        } else {
            let first_page: &Page = pager.get_ref(0);
            let schema = bincode::deserialize(&first_page.borrow()[..]).unwrap();
            dbg!(&schema);
            Self {
                pager,
                schema,
                sources: vec![],
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

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for x in &self.raw {
            write!(f, "{} ", x)?;
        }
        write!(f, "\n")
    }
}

impl Page {
    pub fn new_leaf() -> Self {
        let mut page = Page::from([0; PAGE_SIZE as usize]);
        page[0] = 1;
        page
    }
    pub fn set_parent(&mut self, node_i: usize) {
        self[1..1 + 4].copy_from_slice(&(node_i as u32).to_le_bytes());
    }
    pub fn set_size(&mut self, size: usize) {
        self[1 + 4..1 + 4 + 2].copy_from_slice(&(size as u16).to_le_bytes());
    }
    pub fn set_next(&mut self, node_i: usize) {
        self[1 + 4 + 2..1 + 4 + 2 + 4].copy_from_slice(&(node_i as u32).to_le_bytes());
    }
    #[inline]
    pub fn slice(&self, offset: usize, size: usize) -> &[u8] {
        &self[offset..offset + size]
    }
    #[inline]
    pub fn slice_mut(&mut self, offset: usize, size: usize) -> &mut [u8] {
        &mut self[offset..offset + size]
    }
    #[inline]
    pub fn write(&mut self, offset: usize, bytes: &[u8]) {
        self[offset..offset + bytes.len()].copy_from_slice(bytes);
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
        indices: vec![],
    });

    f.pager.save();
}
