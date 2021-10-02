pub mod file;
pub mod in_memory;
// pub mod in_memory_old;

use crate::{
    data::Data,
    schema::{self, Schema},
};

pub trait StorageOld: 'static {
    type Cursor;

    fn source_index(&self, source: &str) -> Option<usize>;
    fn get_const_cursor_just(&self, source: usize, key: Data) -> Self::Cursor;
    fn get_const_cursor_range(&self, source: usize, start: usize, end: usize) -> Self::Cursor;
    fn get_from_cursor(&self, cursor: &Self::Cursor) -> Vec<Data>;
    fn advance_cursor(&self, cursor: &mut Self::Cursor) -> bool;
    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool;

    fn push_row(&mut self, source: usize, data: Vec<Data>) -> Result<(), String>;
}

pub trait Storage: 'static {
    type Cursor;
    type SourceIndex: Clone + Copy;

    fn schema(&self) -> &Schema;
    fn add_table(&mut self, table: schema::Table);

    fn source_index(
        &self,
        table_name: &str,
        key_columns: &[String],
    ) -> Option<Self::SourceIndex>;
    fn get_cursor_first(&self, source_index: Self::SourceIndex) -> Self::Cursor;
    fn get_cursor_just(&self, source_index: Self::SourceIndex, key: &Vec<Data>) -> Self::Cursor;

    fn cursor_get_row(&self, cursor: &Self::Cursor) -> Option<Vec<Data>>;
    fn cursor_advance(&self, cursor: &mut Self::Cursor) -> bool;
    fn cursor_is_end(&self, cursor: &Self::Cursor) -> bool;
    fn cursor_delete(&self, cursor: &mut Self::Cursor) -> bool;
    fn cursor_update(&self, cursor: &mut Self::Cursor, data: Vec<Data>) -> bool;

    fn add_row(&mut self, table_name: &str, data: Vec<Data>) -> Result<(), String>;
}

// full scan
// unique row
// multiple rows
