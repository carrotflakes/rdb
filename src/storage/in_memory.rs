use crate::{data::Data, schema::Schema};

use super::Storage;

#[derive(Debug)]
pub struct InMemory {
    schema: Schema,
    tables: Vec<Source>,
    auto_increment: u64,
}

#[derive(Debug)]
pub struct Source {
    table_name: String,
    key_columns: Vec<String>,
    keys: Vec<Data>,
    rows: SourceRows,
}

#[derive(Debug)]
pub enum SourceRows {
    Data {
        columns_num: usize,
        data_vec: Vec<Data>,
    },
    Index {
        source_index: usize,
        indices: Vec<usize>,
    },
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
        self.tables.push(Source {
            table_name: table.name.clone(),
            key_columns: vec![table.columns[table.primary_key.unwrap_or(0)].name.clone()],
            keys: vec![],
            rows: SourceRows::Data {
                columns_num: table.columns.len(),
                data_vec: vec![],
            },
        });
        self.schema.tables.push(table);
    }
    
    fn issue_auto_increment(&mut self, table_name: &str, column_name: &str) -> u64 {
        self.auto_increment +=1;
        self.auto_increment
    }

    fn source_index(&self, table_name: &str, key_columns: &[String]) -> Option<Self::SourceIndex> {
        self.tables
            .iter()
            .position(|x| x.table_name == table_name && x.key_columns == key_columns)
    }

    fn get_cursor_first(&self, source_index: Self::SourceIndex) -> Self::Cursor {
        InMemoryCursor {
            source_index,
            index: 0,
        }
    }

    fn get_cursor_just(&self, source_index: Self::SourceIndex, key: &Vec<Data>) -> Self::Cursor {
        let table = &self.tables[source_index];
        let columns_num = key.len();
        let rows_num = table.keys.len() / columns_num;
        let mut index = 0;
        while index < rows_num {
            if &table.keys[index * columns_num..(index + 1) * columns_num] >= key {
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
        let table = &self.tables[cursor.source_index];
        if cursor.index * table.key_columns.len() >= table.keys.len() {
            None
        } else {
            match &table.rows {
                SourceRows::Data {
                    columns_num,
                    data_vec,
                } => Some(
                    data_vec[cursor.index * columns_num..(cursor.index + 1) * columns_num].to_vec(),
                ),
                SourceRows::Index {
                    source_index,
                    indices,
                } => todo!(),
            }
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
        let table = &self.tables[cursor.source_index];
        cursor.index * table.key_columns.len() >= table.keys.len()
    }

    fn cursor_delete(&self, cursor: &mut Self::Cursor) -> bool {
        todo!()
    }

    fn cursor_update(&self, cursor: &mut Self::Cursor, data: Vec<Data>) -> bool {
        todo!()
    }

    fn add_row(&mut self, table_name: &str, data: Vec<Data>) -> Result<(), String> {
        let st = self.schema.get_table(table_name).unwrap().1;
        for table in self.tables.iter_mut() {
            if table.table_name != table_name {
                continue;
            }
            let keys_num = table.keys.len() / table.key_columns.len();
            let key: Vec<_> = table
                .key_columns
                .iter()
                .map(|c| data[st.get_column(c).unwrap().0].clone())
                .collect();
            let index = table
                .keys
                .chunks(table.key_columns.len())
                .position(|k| key.as_slice() <= k)
                .unwrap_or(keys_num);
            table.keys.splice(index * key.len()..index * key.len(), key);
            match &mut table.rows {
                SourceRows::Data {
                    columns_num,
                    data_vec,
                } => {
                    data_vec.splice(index * *columns_num..index * *columns_num, data.clone());
                }
                SourceRows::Index {
                    source_index,
                    indices,
                } => todo!(),
            }
        }
        Ok(())
    }

    fn flush(&self) {
        println!("InMemory flushed!");
    }
}

impl InMemory {
    pub fn new() -> Self {
        InMemory {
            schema: Schema::new_empty(),
            tables: vec![],
            auto_increment: 1000,
        }
    }
}
