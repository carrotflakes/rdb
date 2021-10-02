use crate::{data::Data, schema::Schema};

use super::Storage;

pub struct InMemory {
    schema: Schema,
    tables: Vec<(String, usize, Vec<Data>)>,
}

pub struct InMemoryCursor {
    source_index: usize,
    index: usize,
}

impl Storage for InMemory {
    type Cursor = InMemoryCursor;
    type SourceIndex = usize;

    fn schema(&self) -> &crate::schema::Schema {
        &self.schema
    }

    fn add_table(&mut self, table: crate::schema::Table) {
        self.tables
            .push((table.name.clone(), table.columns.len(), vec![]));
        self.schema.tables.push(table);
    }

    fn source_index(
        &self,
        table_name: &str,
        key_column_indices: &[usize],
    ) -> Option<Self::SourceIndex> {
        self.tables.iter().position(|x| x.0 == table_name)
    }

    fn get_cursor_first(&self, source_index: Self::SourceIndex) -> Self::Cursor {
        InMemoryCursor {
            source_index,
            index: 0,
        }
    }

    fn get_cursor_just(&self, source_index: Self::SourceIndex, key: &Vec<Data>) -> Self::Cursor {
        // let table = self.tables[source_index];
        // for i in 0..table.2.len() / table.1 {
            
        // }
        
        let table = &self.tables[source_index];
        let columns_num = table.1;
        let rows_num = table.2.len() / columns_num;
        let mut index = 0;
        while index < rows_num {
            if table.2[index * columns_num] == key[0] {
                break;
            }
            index += 1;
        }
        if index == rows_num {
            index = usize::MAX;
        }
        InMemoryCursor {
            source_index,
            index,
        }
    }

    fn cursor_get_row(&self, cursor: &Self::Cursor) -> Option<Vec<Data>> {
        let (_name, columns_num, data_vec) = &self.tables[cursor.source_index];
        if cursor.index * columns_num >= data_vec.len() {
            None
        } else {
            Some(data_vec[cursor.index * columns_num..(cursor.index + 1) * columns_num].to_vec())
        }
    }

    fn cursor_advance(&self, cursor: &mut Self::Cursor) -> bool {
        cursor.index += 1;
        true
    }

    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool {
        if cursor.index == usize::MAX {
            return true;
        }
        let (_name, columns_num, data_vec) = &self.tables[cursor.source_index];
        cursor.index * columns_num >= data_vec.len()
    }

    fn cursor_delete(&self, cursor: &mut Self::Cursor) -> bool {
        todo!()
    }

    fn cursor_update(&self, cursor: &mut Self::Cursor, data: Vec<Data>) -> bool {
        todo!()
    }

    fn add_row(&mut self, source_index: Self::SourceIndex, data: Vec<Data>) -> Result<(), String> {
        self.tables[source_index].2.extend(data);
        Ok(())
    }
}

impl InMemory {
    pub fn new() -> Self {
        InMemory {
            schema: Schema::new_empty(),
            tables: vec![],
        }
    }
}
