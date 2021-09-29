use crate::data::Type;

#[derive(Clone)]
pub struct Schema {
    pub tables: Vec<Table>,
}

#[derive(Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Clone)]
pub struct Column {
    pub name: String,
    pub dtype: Type,
}

impl Schema {
    pub fn get_table_ref(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == name)
    }

    pub fn get_table_index(&self, name: &str) -> Option<usize> {
        self.tables.iter().position(|t| t.name == name)
    }

    pub fn get_table(&self, name: &str) -> Option<(usize, &Table)> {
        if let Some(i) = self.tables.iter().position(|t| t.name == name) {
            Some((i, &self.tables[i]))
        } else {
            None
        }
    }

    pub fn get_column(&self, name: &str, prefer_table: Option<&str>) -> Option<(usize, &Table, usize, &Column)> {
        if let Some(t) = prefer_table {
            if let Some((table_idx, table)) = self.get_table(t) {
                for (i, column) in table.columns.iter().enumerate() {
                    if column.name == name {
                        return Some((table_idx, table, i, column))
                    }
                }
            }
        }

        for (i, table) in self.tables.iter().enumerate() {
            for (j, column) in table.columns.iter().enumerate() {
                if column.name == name {
                    return Some((i, table,j , column))
                }
            }
        }
        None
    }
}
