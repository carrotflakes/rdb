mod impl_btree;
mod pager;

use crate::{data::Data, storage::Storage};

use self::pager::Pager;

pub struct File {
    pager: Pager,
    sources: Vec<String>,
}

pub enum FileCursor {
    Range {
        table_index: usize,
        index: usize,
        end: usize,
    },
    Just {
        table_index: usize,
        index: usize,
        key: Data,
    },
}

impl Storage for File {
    type Cursor = FileCursor;

    fn source_index(&self, source: &str) -> Option<usize> {
        self.sources.iter().position(|x| x == source)
    }

    fn get_const_cursor_just(&self, source: usize, key: Data) -> Self::Cursor {
        // self.pager.get_ref(source)
        todo!()
    }

    fn get_const_cursor_range(&self, source: usize, start: usize, end: usize) -> Self::Cursor {
        todo!()
    }

    fn get_from_cursor(&self, cursor: &Self::Cursor) -> Vec<Data> {
        todo!()
    }

    fn advance_cursor(&self, cursor: &mut Self::Cursor) -> bool {
        todo!()
    }

    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool {
        todo!()
    }

    fn push_row(&mut self, source: usize, data: Vec<Data>) -> Result<(), String> {
        // self.pager.get_mut(source)
        todo!()
    }
}

impl File {
    pub fn open(filepath: &str) -> Self {
        Self {
            pager: Pager::open(filepath),
            sources: vec!["message".to_owned(), "user".to_owned()],
        }
    }
}
