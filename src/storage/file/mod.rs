mod impl_btree;
mod pager;

use std::borrow::Borrow;

use crate::{
    btree::{BTree, BTreeNode},
    data::Data,
    schema::Schema,
    storage::Storage,
};

use self::pager::{Page, PageRaw, Pager};

pub struct File {
    pager: Pager<PageImpl>,
    schema: Schema,
    source_page_indices: Vec<usize>,
}

pub struct FileCursor {
    source_index: usize,
    index: usize,
    end: bool,
}

impl Storage for File {
    type Cursor = FileCursor;
    type SourceIndex = usize;

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn add_table(&mut self, table: crate::schema::Table) {
        self.schema.tables.push(table);
        let page = &mut self.pager.get_mut(0)[..];
        dbg!(bincode::serialize_into(page, &self.schema)).unwrap(); // todo: over size

        let page_index = self.add_root_node();
        self.source_page_indices.push(page_index); // ??
    }

    fn source_index(
        &self,
        table_name: &str,
        key_column_indices: &[usize],
    ) -> Option<Self::SourceIndex> {
        let (i, _table) = self.schema.get_table(table_name)?;
        Some(self.source_page_indices[i])
        // todo!()
    }

    fn get_cursor_just(&self, source_index: Self::SourceIndex, key: &[Data]) -> Self::Cursor {
        // let index;
        // self.find(source_index, key)
        // FileCursor {
        //     source_index,
        //     index,
        //     end: false,
        // }
        todo!()
    }

    fn cursor_get_row(&self, cursor: &Self::Cursor) -> Vec<Data> {
        todo!()
    }

    fn cursor_advance(&self, cursor: &mut Self::Cursor) -> bool {
        todo!()
    }

    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool {
        todo!()
    }

    fn cursor_delete(&self, cursor: &mut Self::Cursor) -> bool {
        todo!()
    }

    fn cursor_update(&self, cursor: &mut Self::Cursor, data: Vec<Data>) -> bool {
        todo!()
    }

    fn add_row(&mut self, source_index: Self::SourceIndex, data: Vec<Data>) -> Result<(), String> {
        todo!()
    }
}

impl File {
    pub fn open(filepath: &str) -> Self {
        let mut pager = Pager::open(filepath);
        if pager.size() == 0 {
            // initialize
            pager.get_ref(0);
            Self {
                pager,
                schema: Schema::new_empty(),
                source_page_indices: vec![],
            }
        } else {
            let first_page: &PageImpl = pager.get_ref(0);
            let schema = bincode::deserialize(&first_page.borrow()[..]).unwrap();
            dbg!(&schema);
            Self {
                pager,
                schema,
                source_page_indices: vec![],
            }
        }
    }
}

pub struct PageImpl {
    raw: PageRaw,
}

impl From<PageRaw> for PageImpl {
    fn from(raw: PageRaw) -> Self {
        PageImpl {raw}
    }
}

impl std::ops::Deref for PageImpl {
    type Target = PageRaw;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl std::ops::DerefMut for PageImpl {
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
    });

    f.pager.save();
}