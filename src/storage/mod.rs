pub mod file;
pub mod in_memory;

use crate::{
    data::Data,
    schema::{self, Schema},
};

pub trait Storage: 'static {
    type Cursor;
    type SourceIndex: Clone + Copy;

    fn schema(&self) -> &Schema;
    fn add_table(&mut self, table: schema::Table);

    fn source_index(&self, table_name: &str, key_columns: &[String]) -> Option<Self::SourceIndex>;
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
