use std::collections::BTreeMap;

use crate::iterate::Iterate;

pub struct InMemory {
    records: Vec<Vec<Vec<u8>>>,
    indices: Vec<Index>,
}

struct Index {
    record_index: usize,
    index: BTreeMap<Vec<u8>, usize>,
}

#[derive(Debug, Clone)]
pub struct Cursor {
    iterate_index: usize,
    cursor: usize,
}

impl InMemory {
    pub fn new() -> Self {
        Self {
            records: vec![],
            indices: vec![],
        }
    }
}

impl Iterate for InMemory {
    type IterateIndex = usize;
    type Item = Vec<u8>;
    type Cursor = Cursor;

    fn first_cursor(&self, iterate_index: Self::IterateIndex) -> Self::Cursor {
        Cursor {
            iterate_index,
            cursor: 0,
        }
    }

    fn find(&self, iterate_index: Self::IterateIndex, item: &Self::Item) -> Self::Cursor {
        let index = self.indices[iterate_index];
        // index.index.get(item).map(|cursor| Cursor {
        //     iterate_index,
        //     cursor: *cursor,
        // })
    }

    fn cursor_get(&self, cursor: &Self::Cursor) -> Option<Self::Item> {
        todo!()
    }

    fn cursor_next(&self, cursor: &mut Self::Cursor) -> bool {
        todo!()
    }

    fn cursor_next_occupied(&self, cursor: &Self::Cursor) -> Self::Cursor {
        todo!()
    }

    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool {
        todo!()
    }

    fn cursor_delete(&mut self, cursor: &Self::Cursor) -> bool {
        todo!()
    }

    fn cursor_update(&mut self, cursor: &Self::Cursor, item: Self::Item) -> bool {
        todo!()
    }

    fn add(&mut self, iterate_index: Self::IterateIndex, item: Self::Item) -> bool {
        todo!()
    }
}
