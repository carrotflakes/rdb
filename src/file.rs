// テーブルとキー、得たいものを選んでカーソル操作を提供する
// ファイルへのserialize, deserialize
// テーブルの追加、削除、変更
// インデックスの追加
// 行のCRUD操作

use crate::{btree::{BTree, BTreeCursor}, data::{data_vec_from_bytes, data_vec_to_bytes, Data, Type}, disk_cache::DiskCache, disk_cache_btree::Meta, iterate::Iterate, object_store::*, record_store::{self, RecordStore}, schema::{self, Schema}, util::bytes::parse_u32};

pub struct File {
    disk_cache: DiskCache,
    schema: Schema,
    tables: Vec<Table>,
    iterators: Vec<Iterator>,
}

pub struct Table {
    record_store: RecordStore,
}

#[derive(Debug, Clone)]
pub struct FileCursor {
    iterator_index: IteratorIndex,
    cursor: Cursor,
}

#[derive(Debug, Clone)]
pub enum Cursor {
    BTreeCursor(BTreeCursor),
    RecordStoreCursor(record_store::Cursor),
}

#[derive(Debug, Clone)]
pub struct Iterator {
    table_index: usize,
    index_info: Option<IndexInfo>,
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    key_columns: Vec<String>,
    key_column_indices: Vec<usize>,
    key_types: Vec<Type>,
    node_id: usize,
    btree_meta: Meta,
}

#[derive(Debug, Clone, Copy)]
pub struct IteratorIndex(usize);

impl File {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn add_table(&mut self, table: schema::Table) {
        let table_index = self.schema.tables.len();

        // data record
        self.iterators.push(Iterator {
            table_index,
            index_info: None,
        });

        // primary key
        {
            let key_columns = table
                .primary_key
                .iter()
                .map(|ci| &table.columns[*ci])
                .collect::<Vec<_>>();

            let node_id = self.disk_cache.add_root_node();

            self.iterators.push(Iterator {
                table_index,
                index_info: Some(IndexInfo {
                    key_columns: key_columns.iter().map(|c| c.name.clone()).collect(),
                    key_column_indices: table.primary_key.to_vec(),
                    key_types: key_columns.iter().map(|c| c.dtype.clone()).collect(),
                    node_id,
                    btree_meta: Meta {
                        key_size: key_columns.iter().map(|c| c.dtype.size()).sum(),
                        value_size: Some(4),
                    },
                }),
            });
        }

        let value_size = table.columns.iter().map(|c| c.dtype.size()).sum();
        self.tables.push(Table {
            record_store: RecordStore::new(self.disk_cache.next_page_id(), value_size),
        });
        self.schema.tables.push(table);
    }

    fn iterator_index(
        &self,
        table_name: &str,
        key_columns: Option<&[String]>,
    ) -> Option<IteratorIndex> {
        for (i, iterator) in self.iterators.iter().enumerate() {
            let table = &self.schema.tables[iterator.table_index];
            if table.name == table_name
                && iterator
                    .index_info
                    .as_ref()
                    .map(|ii| ii.key_columns.as_slice())
                    == key_columns
            {
                return Some(IteratorIndex(i));
            }
        }
        None
    }

    fn get_cursor_first(&self, iterator_index: IteratorIndex) -> FileCursor {
        let iterator = &self.iterators[iterator_index.0];
        let cursor = if let Some(index_info) = &iterator.index_info {
            Cursor::BTreeCursor(
                self.disk_cache
                    .first_cursor(&index_info.btree_meta, index_info.node_id),
            )
        } else {
            let record_store = &self.tables[iterator.table_index].record_store;
            Cursor::RecordStoreCursor(record_store.first_cursor())
        };

        FileCursor {
            iterator_index,
            cursor,
        }
    }

    fn get_cursor_just(&self, iterator_index: IteratorIndex, key: &Vec<u8>) -> FileCursor {
        let iterator = &self.iterators[iterator_index.0];
        let cursor = if let Some(index_info) = &iterator.index_info {
            Cursor::BTreeCursor(
                self.disk_cache
                    .find(&index_info.btree_meta, index_info.node_id, key)
                    .0,
            )
        } else {
            // TODO?
            let record_store = &self.tables[iterator.table_index].record_store;
            Cursor::RecordStoreCursor(record_store.first_cursor())
        };

        FileCursor {
            iterator_index,
            cursor,
        }
    }

    fn cursor_get_row(&self, cursor: &FileCursor) -> Option<Vec<Data>> {
        let iterator = &self.iterators[cursor.iterator_index.0];
        let table = &self.schema.tables[iterator.table_index];
        let record_store = &self.tables[iterator.table_index].record_store;
        let cursor = if let Some(index_info) = &iterator.index_info {
            match &cursor.cursor {
                Cursor::BTreeCursor(cursor) => {
                    let (_, value) = self.disk_cache.cursor_get(&index_info.btree_meta, cursor)?;
                    record_store::Cursor::from(parse_u32(&value))
                }
                _ => panic!(),
            }
        } else {
            match &cursor.cursor {
                Cursor::RecordStoreCursor(cursor) => cursor.clone(),
                _ => panic!(),
            }
        };

        record_store
            .cursor_get(&self.disk_cache, &cursor)
            .and_then(|bytes| {
                data_vec_from_bytes(
                    &table
                        .columns
                        .iter()
                        .map(|c| c.dtype.clone())
                        .collect::<Vec<_>>(),
                    &bytes,
                )
            })
    }

    fn cursor_next(&self, cursor: &mut FileCursor) -> bool {
        let iterator = &self.iterators[cursor.iterator_index.0];
        if let Some(index_info) = &iterator.index_info {
            match &mut cursor.cursor {
                Cursor::BTreeCursor(cursor) => {
                    *cursor = self
                        .disk_cache
                        .cursor_next(&index_info.btree_meta, cursor.clone());
                }
                _ => panic!(),
            }
        } else {
            match &mut cursor.cursor {
                Cursor::RecordStoreCursor(cursor) => {
                    let record_store = &self.tables[iterator.table_index].record_store;
                    *cursor = record_store.cursor_next(&self.disk_cache, cursor);
                }
                _ => panic!(),
            }
        }
        true
    }

    fn cursor_is_end(&self, cursor: &FileCursor) -> bool {
        let iterator = &self.iterators[cursor.iterator_index.0];
        if let Some(index_info) = &iterator.index_info {
            match &cursor.cursor {
                Cursor::BTreeCursor(cursor) => self
                    .disk_cache
                    .cursor_is_end(&index_info.btree_meta, cursor),
                _ => panic!(),
            }
        } else {
            match &cursor.cursor {
                Cursor::RecordStoreCursor(cursor) => {
                    let record_store = &self.tables[iterator.table_index].record_store;
                    record_store.cursor_is_end(&self.disk_cache, cursor)
                }
                _ => panic!(),
            }
        }
    }

    // fn cursor_next_occupied(&self, cursor: &mut FileCursor);
    // fn cursor_delete(&mut self, cursor: &mut FileCursor) -> bool;
    // fn cursor_update(&self, cursor: &mut FileCursor, data: Vec<Data>) -> bool;

    fn add_row(&mut self, table_name: &str, data: Vec<Data>) -> Result<(), String> {
        let (table_index, _) = self.schema.get_table(table_name).unwrap();
        let record_store = &mut self.tables[table_index].record_store;
        let cursor = record_store.push_record(&mut self.disk_cache, &data_vec_to_bytes(&data));

        // update indices
        for iterator in self.iterators.iter() {
            if iterator.table_index != table_index {
                continue;
            }
            if let Some(index_info) = &iterator.index_info {
                let key: Vec<_> = index_info
                    .key_column_indices
                    .iter()
                    .map(|i| data[*i].clone())
                    .collect();
                let key = data_vec_to_bytes(&key);
                let value = u32::from(cursor).to_le_bytes().to_vec();
                self.disk_cache
                    .insert(&index_info.btree_meta, index_info.node_id, &key, &value)
                    .unwrap();
            }
        }

        Ok(())
    }

    // fn flush(&self);
}

impl Iterate for File {
    type IterateIndex = IteratorIndex;
    type Item = Vec<u8>;
    type Cursor = FileCursor;

    fn first_cursor(&self, iterate_index: Self::IterateIndex) -> Self::Cursor {
        let iterator = &self.iterators[iterate_index.0];
        let cursor = if let Some(index_info) = &iterator.index_info {
            Cursor::BTreeCursor(
                self.disk_cache
                    .first_cursor(&index_info.btree_meta, index_info.node_id),
            )
        } else {
            let record_store = &self.tables[iterator.table_index].record_store;
            Cursor::RecordStoreCursor(record_store.first_cursor())
        };

        FileCursor {
            iterator_index: iterate_index,
            cursor,
        }
    }

    fn find(&self, iterate_index: Self::IterateIndex, item: &Self::Item) -> Self::Cursor {
       
        let iterator = &self.iterators[iterate_index.0];
        let cursor = if let Some(index_info) = &iterator.index_info {
            Cursor::BTreeCursor(
                self.disk_cache
                    .find(&index_info.btree_meta, index_info.node_id, item)
                    .0,
            )
        } else {
            // TODO?
            let record_store = &self.tables[iterator.table_index].record_store;
            Cursor::RecordStoreCursor(record_store.first_cursor())
        };

        FileCursor {
            iterator_index: iterate_index,
            cursor,
        }
    }

    fn cursor_get(&self, cursor: &Self::Cursor) -> Option<Self::Item> {
        let iterator = &self.iterators[cursor.iterator_index.0];
        let table = &self.schema.tables[iterator.table_index];
        let record_store = &self.tables[iterator.table_index].record_store;
        let cursor = if let Some(index_info) = &iterator.index_info {
            match &cursor.cursor {
                Cursor::BTreeCursor(cursor) => {
                    let (_, value) = self.disk_cache.cursor_get(&index_info.btree_meta, cursor)?;
                    record_store::Cursor::from(parse_u32(&value))
                }
                _ => panic!(),
            }
        } else {
            match &cursor.cursor {
                Cursor::RecordStoreCursor(cursor) => cursor.clone(),
                _ => panic!(),
            }
        };

        record_store
            .cursor_get(&self.disk_cache, &cursor)
            // .and_then(|bytes| {
            //     data_vec_from_bytes(
            //         &table
            //             .columns
            //             .iter()
            //             .map(|c| c.dtype.clone())
            //             .collect::<Vec<_>>(),
            //         &bytes,
            //     )
            // })
    }

    fn cursor_next(&self, cursor: &mut Self::Cursor) -> bool {
        let iterator = &self.iterators[cursor.iterator_index.0];
        if let Some(index_info) = &iterator.index_info {
            match &mut cursor.cursor {
                Cursor::BTreeCursor(cursor) => {
                    *cursor = self
                        .disk_cache
                        .cursor_next(&index_info.btree_meta, cursor.clone());
                }
                _ => panic!(),
            }
        } else {
            match &mut cursor.cursor {
                Cursor::RecordStoreCursor(cursor) => {
                    let record_store = &self.tables[iterator.table_index].record_store;
                    *cursor = record_store.cursor_next(&self.disk_cache, cursor);
                }
                _ => panic!(),
            }
        }
        true
    }

    fn cursor_next_occupied(&self, cursor: &Self::Cursor) -> Self::Cursor {
        todo!()
    }

    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool {
        let iterator = &self.iterators[cursor.iterator_index.0];
        if let Some(index_info) = &iterator.index_info {
            match &cursor.cursor {
                Cursor::BTreeCursor(cursor) => self
                    .disk_cache
                    .cursor_is_end(&index_info.btree_meta, cursor),
                _ => panic!(),
            }
        } else {
            match &cursor.cursor {
                Cursor::RecordStoreCursor(cursor) => {
                    let record_store = &self.tables[iterator.table_index].record_store;
                    record_store.cursor_is_end(&self.disk_cache, cursor)
                }
                _ => panic!(),
            }
        }
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

impl File {
    pub fn open(filepath: &str) -> Self {
        let mut disk = crate::disk::Disk::open(filepath);
        let initialized = disk.next_page_id() != 0;
        let disk = std::sync::Arc::new(std::sync::Mutex::new(disk));
        let mut disk_cache = crate::disk_cache::DiskCache::new(disk);
        if initialized {
            let schema = read_object(&mut disk_cache, "schema").unwrap();
            dbg!(&schema);
            Self {
                disk_cache,
                schema,
                tables: vec![],
                iterators: vec![],
            }
        } else {
            // initialize
            let schema = Schema::new_empty();
            init_as_object_store(&mut disk_cache);
            write_object(&mut disk_cache, "schema", &schema);

            Self {
                disk_cache,
                schema,
                tables: vec![],
                iterators: vec![],
            }
        }
    }

    pub fn write_schema(&mut self) {
        write_object(&mut self.disk_cache, "schema", &self.schema);
    }
}

#[test]
fn test() {
    let mut f = File::open("hello");
    f.add_table(crate::schema::Table {
        name: "hey".to_owned(),
        columns: vec![crate::schema::Column {
            name: "id".to_owned(),
            dtype: Type::U64,
            default: None,
        }],
        primary_key: vec![0],
        constraints: vec![],
        indices: vec![],
    });

    f.add_row("hey", vec![Data::U64(123)]).unwrap();
    f.add_row("hey", vec![Data::U64(45)]).unwrap();
    f.add_row("hey", vec![Data::U64(6)]).unwrap();
    f.add_row("hey", vec![Data::U64(54)]).unwrap();
    f.add_row("hey", vec![Data::U64(321)]).unwrap();

    let mut cursor = f.get_cursor_first(f.iterator_index("hey", None).unwrap());
    dbg!(f.cursor_get_row(&cursor));
    dbg!(f.cursor_next(&mut cursor));
    dbg!(f.cursor_get_row(&cursor));

    let mut cursor = f.get_cursor_first(f.iterator_index("hey", Some(&["id".to_owned()])).unwrap());
    dbg!(f.cursor_get_row(&cursor));
    dbg!(f.cursor_next(&mut cursor));
    dbg!(f.cursor_get_row(&cursor));
}
