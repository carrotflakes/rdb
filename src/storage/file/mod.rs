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
    source_page_indices: Vec<usize>,
    key_types: Vec<Vec<Type>>,
    value_types: Vec<Vec<Type>>,
    metas: Vec<Meta>,
    auto_increment: u64,
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
        dbg!(bincode::serialize_into(page, &self.schema)).unwrap(); // todo: over size

        let page_index = self.add_root_node();
        self.source_page_indices.push(page_index); // ??
        self.key_types
            .push(if let Some(primary_key) = table.primary_key {
                vec![table.columns[primary_key].dtype.clone()]
            } else {
                vec![]
            });
        self.value_types
            .push(table.columns.iter().map(|c| c.dtype.clone()).collect());
        self.metas.push(Meta {
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
        });

        self.schema.tables.push(table);
    }

    fn issue_auto_increment(&mut self, table_name: &str, column_name: &str) -> u64 {
        self.auto_increment += 1;
        self.auto_increment
    }

    fn source_index(&self, table_name: &str, key_columns: &[String]) -> Option<Self::SourceIndex> {
        let (i, _table) = self.schema.get_table(table_name)?;
        Some(i)
        // todo!()
    }

    fn get_cursor_first(&self, source_index: Self::SourceIndex) -> Self::Cursor {
        FileCursor {
            source_index,
            btree_cursor: self.first_cursor(
                &self.metas[source_index],
                self.source_page_indices[source_index],
            ),
        }
    }

    fn get_cursor_just(&self, source_index: Self::SourceIndex, key: &Vec<Data>) -> Self::Cursor {
        let key = data_vec_to_bytes(key);
        FileCursor {
            source_index,
            btree_cursor: self
                .find(
                    &self.metas[source_index],
                    self.source_page_indices[source_index],
                    &key,
                )
                .unwrap(),
        }
    }

    fn cursor_get_row(&self, cursor: &Self::Cursor) -> Option<Vec<Data>> {
        self.cursor_get(&self.metas[cursor.source_index], &cursor.btree_cursor)
            .map(|x| data_vec_from_bytes(&self.value_types[cursor.source_index], &x.1).unwrap())
    }

    fn cursor_advance(&self, cursor: &mut Self::Cursor) -> bool {
        let c = self.cursor_next(&self.metas[cursor.source_index], &cursor.btree_cursor);
        cursor.btree_cursor = c;
        true
    }

    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool {
        <Self as BTree<Vec<u8>, Vec<u8>>>::cursor_is_end(
            &self,
            &self.metas[cursor.source_index],
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
        let r = self.insert(&meta, node_i, &key, &value);
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
                source_page_indices: vec![],
                key_types: vec![],
                value_types: vec![],
                metas: vec![],
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
                value_types: vec![],
                metas: vec![],
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
    pub fn new_internal(parent: Option<usize>) -> Self {
        Page::from([0; PAGE_SIZE as usize])
    }
    pub fn new_leaf(parent: Option<usize>) -> Self {
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
    pub fn slice_mut(&self, offset: usize, size: usize) -> &[u8] {
        &self[offset..offset + size]
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
    });

    f.pager.save();
}
