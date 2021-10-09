use crate::{
    data::{Data, Type},
    schema::{self, Column, Table},
};

pub fn new_auto_increment_table() -> Table {
    Table {
        name: "auto_increment".to_owned(),
        columns: vec![
            Column {
                name: "table".to_owned(),
                dtype: Type::String,
                default: None,
            },
            Column {
                name: "column".to_owned(),
                dtype: Type::String,
                default: None,
            },
            Column {
                name: "num".to_owned(),
                dtype: Type::U64,
                default: Some(schema::Default::Data(Data::U64(1))),
            },
        ],
        primary_key: vec![0, 1],
        constraints: vec![],
        indices: vec![],
    }
}
