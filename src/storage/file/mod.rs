mod impl_btree;
mod page;
mod pager;
mod simple_store;
mod summary;

use crate::{
    btree::{BTree, BTreeCursor},
    data::{data_vec_from_bytes, data_vec_to_bytes, Data, Type},
    schema::Schema,
    storage::{
        file::simple_store::{init_as_simple_store, read_object, write_object},
        Storage,
    },
};

use self::{impl_btree::Meta, pager::Pager};

pub struct File {
    pager: Pager<page::Page>,
    schema: Schema,
    sources: Vec<Source>,
}

pub struct Source {
    table_index: usize,
    page_index: usize,
    key_columns: Vec<String>,
    key_column_indices: Vec<usize>,
    value_column_indices: Vec<usize>,
    key_types: Vec<Type>,
    value_types: Vec<Type>,
    parent_source_index: Option<usize>,
    meta: Meta,
}

#[derive(Debug, Clone)]
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
        // TODO: name duplication check
        let source_index = self.sources.len();
        let page_index = self.pager.add_root_node();
        let value_column_indices: Vec<_> = (0..table.columns.len())
            .filter(|x| !table.primary_key.contains(x))
            .collect();
        let primary_key_types: Vec<_> = table
            .primary_key
            .iter()
            .map(|i| table.columns[*i].dtype.clone())
            .collect();
        self.sources.push(Source {
            table_index: self.schema.tables.len(),
            page_index,
            key_columns: table
                .primary_key
                .iter()
                .map(|i| table.columns[*i].name.clone())
                .collect(),
            key_column_indices: table.primary_key.clone(),
            value_column_indices: value_column_indices.clone(),
            key_types: primary_key_types.clone(),
            value_types: value_column_indices
                .iter()
                .map(|i| table.columns[*i].dtype.clone())
                .collect(),
            parent_source_index: None,
            meta: Meta {
                key_size: table
                    .primary_key
                    .iter()
                    .map(|i| table.columns[*i].dtype.size())
                    .sum(),
                value_size: value_column_indices
                    .iter()
                    .map(|i| table.columns[*i].dtype.size())
                    .sum(),
            },
        });

        // sources for indices
        for cols in table.indices.iter().map(|i| &i.column_indices) {
            let key_columns = cols
                .iter()
                .map(|ci| &table.columns[*ci])
                .collect::<Vec<_>>();
            let page_index = self.pager.add_root_node();
            self.sources.push(Source {
                table_index: self.schema.tables.len(),
                page_index,
                key_column_indices: cols.clone(),
                value_column_indices: vec![],
                key_types: cols
                    .iter()
                    .map(|i| table.columns[*i].dtype.clone())
                    .collect(),
                key_columns: key_columns.iter().map(|c| c.name.clone()).collect(),
                value_types: primary_key_types.clone(),
                parent_source_index: Some(source_index),
                meta: Meta {
                    key_size: key_columns.iter().map(|c| c.dtype.size()).sum(),
                    value_size: primary_key_types.iter().map(|t| t.size()).sum(),
                },
            });
        }

        self.schema.tables.push(table);
        self.write_schema();
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
        let (btree_cursor, _found) = self.pager.find(&source.meta, source.page_index, &key);
        FileCursor {
            source_index,
            btree_cursor,
        }
    }

    fn cursor_get_row(&self, cursor: &Self::Cursor) -> Option<Vec<Data>> {
        let mut source = &self.sources[cursor.source_index];
        let (key, value) = self.pager.cursor_get(&source.meta, &cursor.btree_cursor)?;
        let (key, value) = if let Some(parent_surce_index) = source.parent_source_index {
            source = &self.sources[parent_surce_index];
            let (btree_cursor, found) = self.pager.find(&source.meta, source.page_index, &value);
            assert!(found, "row is not found, index is broken?");
            self.pager.cursor_get(&source.meta, &btree_cursor)?
        } else {
            (key, value)
        };
        Some(source.build_value(&key, &value))
    }

    fn cursor_advance(&self, cursor: &mut Self::Cursor) -> bool {
        let source = &self.sources[cursor.source_index];
        cursor.btree_cursor = self
            .pager
            .cursor_next(&source.meta, cursor.btree_cursor.clone());
        true
    }

    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool {
        let source = &self.sources[cursor.source_index];
        self.pager.cursor_is_end(&source.meta, &cursor.btree_cursor)
    }

    fn cursor_next_occupied(&self, cursor: &mut Self::Cursor) {
        let source = &self.sources[cursor.source_index];
        cursor.btree_cursor = self
            .pager
            .cursor_next_occupied(&source.meta, cursor.btree_cursor.clone());
    }

    fn cursor_delete(&mut self, cursor: &mut Self::Cursor) -> bool {
        let source = &self.sources[cursor.source_index];
        if let Some(parent_source_index) = source.parent_source_index {
            let (_key, index) = self
                .pager
                .cursor_get(&source.meta, &cursor.btree_cursor)
                .unwrap();
            cursor.btree_cursor = self
                .pager
                .cursor_delete(&source.meta, cursor.btree_cursor.clone());

            let main_source = &self.sources[parent_source_index];
            let (btree_cursor, found) =
                self.pager
                    .find(&main_source.meta, main_source.page_index, &index);
            assert!(found, "row is not found, index is broken?");

            let (key, value) = self
                .pager
                .cursor_get(&main_source.meta, &btree_cursor)
                .unwrap();

            // delete main
            self.pager.cursor_delete(&main_source.meta, btree_cursor);

            let value = main_source.build_value(&key, &value);
            let pk = key;

            // delete indices
            for (source_index, source) in self.sources.iter().enumerate() {
                if source.table_index != main_source.table_index
                    || source_index == cursor.source_index
                {
                    continue;
                }

                let key: Vec<_> = source
                    .key_column_indices
                    .iter()
                    .map(|i| value[*i].clone())
                    .collect();
                let key = data_vec_to_bytes(&key);
                let (mut cursor, found) = self.pager.find(&source.meta, source.page_index, &key);
                assert!(found, "index is broken?");

                while self.pager.cursor_get(&source.meta, &cursor).unwrap().1 != pk {
                    cursor = self.pager.cursor_next(&source.meta, cursor);
                }
                self.pager.cursor_delete(&source.meta, cursor);
            }
        } else {
            let (key, value) = self
                .pager
                .cursor_get(&source.meta, &cursor.btree_cursor)
                .unwrap();

            // delete main
            cursor.btree_cursor = self
                .pager
                .cursor_delete(&source.meta, cursor.btree_cursor.clone());

            let value = source.build_value(&key, &value);
            let pk = key;

            // delete indices
            let main_source = source;
            for source in self.sources.iter() {
                if source.table_index != main_source.table_index
                    || source.parent_source_index.is_none()
                {
                    continue;
                }

                let key: Vec<_> = source
                    .key_column_indices
                    .iter()
                    .map(|i| value[*i].clone())
                    .collect();
                let key = data_vec_to_bytes(&key);
                let (mut cursor, found) = self.pager.find(&source.meta, source.page_index, &key);
                assert!(found, "index is broken?");

                while self.pager.cursor_get(&source.meta, &cursor).unwrap().1 != pk {
                    cursor = self.pager.cursor_next(&source.meta, cursor);
                }
                self.pager.cursor_delete(&source.meta, cursor);
            }
        };

        true
    }

    fn cursor_update(&self, cursor: &mut Self::Cursor, data: Vec<Data>) -> bool {
        todo!()
    }

    fn add_row(&mut self, table_name: &str, data: Vec<Data>) -> Result<(), String> {
        let (table_index, table) = self.schema.get_table(table_name).unwrap();

        let index_value: Vec<_> = table.primary_key.iter().map(|i| data[*i].clone()).collect();

        for source in self.sources.iter() {
            if source.table_index != table_index {
                continue;
            }

            let key: Vec<_> = source
                .key_column_indices
                .iter()
                .map(|i| data[*i].clone())
                .collect();
            let key = data_vec_to_bytes(&key);

            let value: Vec<_> = source
                .value_column_indices
                .iter()
                .map(|i| data[*i].clone())
                .collect();
            let value = if source.parent_source_index.is_some() {
                data_vec_to_bytes(&index_value)
            } else {
                data_vec_to_bytes(&value)
            };

            self.pager
                .insert(&source.meta, source.page_index, &key, &value)?; // !!!!
        }
        Ok(())
    }

    fn flush(&self) {
        #[allow(mutable_transmutes)]
        let pager = unsafe { std::mem::transmute::<_, &mut Pager<page::Page>>(&self.pager) };
        pager.save()
    }
}

impl File {
    pub fn open(filepath: &str) -> Self {
        let mut pager = Pager::<page::Page>::open(filepath);
        if pager.size() == 0 {
            // initialize
            let schema = Schema::new_empty();
            init_as_simple_store(&mut pager);
            write_object(&mut pager, "schema", &schema);

            // TODO write file header
            // 0x1006 <storage format>
            Self {
                pager,
                schema,
                sources: vec![],
            }
        } else {
            let schema = read_object(&mut pager, "schema").unwrap();
            dbg!(&schema);
            Self {
                pager,
                schema,
                sources: vec![],
            }
        }
    }

    pub fn write_schema(&mut self) {
        write_object(&mut self.pager, "schema", &self.schema);
    }
}

impl Source {
    pub fn build_value(&self, key: &[u8], value: &[u8]) -> Vec<Data> {
        let key = data_vec_from_bytes(&self.key_types, key).unwrap();
        let value = data_vec_from_bytes(&self.value_types, value).unwrap();
        let mut ret = vec![Data::U64(0); key.len() + value.len()];
        for (i, j) in self.key_column_indices.iter().enumerate() {
            ret[*j] = key[i].clone();
        }
        for (i, j) in self.value_column_indices.iter().enumerate() {
            ret[*j] = value[i].clone();
        }
        ret
    }
}

#[test]
fn test() {
    let mut f = File::open("hello");
    f.add_table(crate::schema::Table {
        name: "hey".to_owned(),
        columns: vec![],
        primary_key: vec![0],
        constraints: vec![],
        indices: vec![],
    });

    f.pager.save();
}
