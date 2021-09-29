use crate::data::Data;

pub trait Storage {
    type Cursor;

    fn source_index(&self, source: &str) -> Option<usize>;
    fn get_const_cursor_range(&self, source: usize, start: usize, end: usize) -> Self::Cursor;
    fn get_from_cursor(&self, cursor: &Self::Cursor) -> Vec<Data>;
    fn advance_cursor(&self, cursor: &mut Self::Cursor) -> bool;

    fn push_row(&mut self, source: usize, data: Vec<Data>) -> Result<(), String>;
}
