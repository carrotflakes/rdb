use crate::{data::Data, storage::Storage};

pub struct InMemory {
    tables: Vec<(String, usize, Vec<Data>)>,
}

pub struct InMemoryCursor {
    table_index: usize,
    index: usize,
    end: usize,
}

impl Storage for InMemory {
    type Cursor = InMemoryCursor;

    fn source_index(&self, source: &str) -> Option<usize> {
        self.tables.iter().position(|x| x.0 == source)
    }

    fn get_const_cursor_range(&self, source: usize, start: usize, end: usize) -> Self::Cursor {
        InMemoryCursor {
            table_index: source,
            index: start,
            end: end.min(self.tables[source].2.len() / self.tables[source].1),
        }
    }

    fn get_from_cursor(&self, cursor: &Self::Cursor) -> Vec<Data> {
        let InMemoryCursor {
            table_index,
            index,
            end: _,
        } = cursor;
        let (_name, columns_num, data_vec) = &self.tables[*table_index];
        data_vec[index * columns_num..(index + 1) * columns_num].to_vec()
    }

    fn advance_cursor(&self, cursor: &mut Self::Cursor) -> bool {
        cursor.index += 1;
        cursor.index < cursor.end
    }

    fn push_row(&mut self, source: usize, data: Vec<Data>) -> Result<(), String> {
        self.tables[source].2.extend(data);
        Ok(())
    }
}

impl InMemory {
    pub fn new() -> Self {
        InMemory { tables: vec![] }
    }

    pub fn add_table(&mut self, name: String, columns_num: usize) {
        self.tables.push((name, columns_num, vec![]));
    }
}
