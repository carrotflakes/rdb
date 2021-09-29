use crate::{data::Data, storage::Storage};

pub struct InMemory {
    tables: Vec<(String, usize, Vec<Data>)>,
}

pub enum InMemoryCursor {
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

impl Storage for InMemory {
    type Cursor = InMemoryCursor;

    fn source_index(&self, source: &str) -> Option<usize> {
        self.tables.iter().position(|x| x.0 == source)
    }

    fn get_const_cursor_just(&self, source: usize, key: Data) -> Self::Cursor {
        let table = &self.tables[source];
        let columns_num = table.1;
        let rows_num = table.2.len() / columns_num;
        let mut index = 0;
        while index < rows_num {
            if table.2[index * columns_num] == key {
                break;
            }
            index += 1;
        }
        if index == rows_num {
            index = usize::MAX;
        }
        InMemoryCursor::Just {
            table_index: source,
            index,
            key,
        }
    }

    fn get_const_cursor_range(&self, source: usize, start: usize, end: usize) -> Self::Cursor {
        InMemoryCursor::Range {
            table_index: source,
            index: start,
            end: end.min(self.tables[source].2.len() / self.tables[source].1),
        }
    }

    fn get_from_cursor(&self, cursor: &Self::Cursor) -> Vec<Data> {
        match cursor {
            InMemoryCursor::Range {
                table_index,
                index,
                end,
            } => {
                let (_name, columns_num, data_vec) = &self.tables[*table_index];
                data_vec[index * columns_num..(index + 1) * columns_num].to_vec()
            }
            InMemoryCursor::Just {
                table_index,
                index,
                key,
            } => {
                let (_name, columns_num, data_vec) = &self.tables[*table_index];
                data_vec[index * columns_num..(index + 1) * columns_num].to_vec()
            }
        }
    }

    fn advance_cursor(&self, cursor: &mut Self::Cursor) -> bool {
        match cursor {
            InMemoryCursor::Range {
                table_index,
                index,
                end,
            } => {
                *index += 1;
                if index < end {
                    true
                } else {
                    *index = usize::MAX;
                    false
                }
            }
            InMemoryCursor::Just {
                table_index,
                index,
                key,
            } => {
                *index += 1;
                let table = &self.tables[*table_index];
                let columns_num = table.1;
                let rows_num = table.2.len() / columns_num;
                if *index < rows_num && table.2[*index * columns_num] == *key {
                    true
                } else {
                    *index = usize::MAX;
                    false
                }
            }
        }
    }

    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool {
        match cursor {
            InMemoryCursor::Range {
                table_index,
                index,
                end,
            } => *index == usize::MAX,
            InMemoryCursor::Just {
                table_index,
                index,
                key,
            } => *index == usize::MAX,
        }
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
