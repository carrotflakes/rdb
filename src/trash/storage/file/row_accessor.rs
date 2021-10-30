use crate::{
    btree::{BTree, BTreeCursor},
    data::{data_vec_from_bytes, data_vec_to_bytes, Data, Type},
};

use super::{impl_btree::Meta, page::Page, pager::Pager};

#[derive(Debug, Clone)]
pub struct RowAccessor {
    page_index: usize,
    key_columns: Vec<String>,
    key_column_indices: Vec<usize>,
    value_column_indices: Vec<usize>,
    key_types: Vec<Type>,
    value_types: Vec<Type>,
    meta: Meta,
}

pub type Cursor = BTreeCursor;

impl RowAccessor {
    pub fn get_cursor_first(&self, pager: &mut Pager<Page>) -> Cursor {
        pager.first_cursor(&self.meta, self.page_index)
    }

    pub fn get_cursor_just(&self, pager: &mut Pager<Page>, key: &Vec<Data>) -> Cursor {
        let key = data_vec_to_bytes(key);
        pager.find(&self.meta, self.page_index, &key).0
    }

    pub fn cursor_get(&self, pager: &mut Pager<Page>, cursor: &Cursor) -> Option<Vec<Data>> {
        let (key, value) = pager.cursor_get(&self.meta, cursor)?;
        Some(self.build_value(&key, &value))
    }

    pub fn cursor_next(&self, pager: &mut Pager<Page>, cursor: &mut Cursor) {
        *cursor = pager.cursor_next(&self.meta, cursor.clone());
    }

    pub fn cursor_next_occupied(&self, pager: &mut Pager<Page>, cursor: &mut Cursor) {
        *cursor = pager.cursor_next_occupied(&self.meta, cursor.clone());
    }

    pub fn cursor_is_end(&self, pager: &mut Pager<Page>, cursor: &Cursor) -> bool {
        pager.cursor_is_end(&self.meta, cursor)
    }

    pub fn cursor_delete(&mut self, pager: &mut Pager<Page>, cursor: &mut Cursor) -> bool {
        *cursor = pager.cursor_delete(&self.meta, cursor.clone());
        true
    }

    pub fn add_row(&mut self, pager: &mut Pager<Page>, data: Vec<Data>) -> Result<(), String> {
        let key: Vec<_> = self
            .key_column_indices
            .iter()
            .map(|i| data[*i].clone())
            .collect();
        let key = data_vec_to_bytes(&key);

        let value: Vec<_> = self
            .value_column_indices
            .iter()
            .map(|i| data[*i].clone())
            .collect();
        let value = data_vec_to_bytes(&value);

        pager.insert(&self.meta, self.page_index, &key, &value)?;
        Ok(())
    }

    fn build_value(&self, key: &[u8], value: &[u8]) -> Vec<Data> {
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
