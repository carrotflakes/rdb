use crate::{
    data::Type,
    schema::{Column, Default, Table},
};

use super::string_to_data;

pub fn parse_table_from_yaml(src: &str) -> Result<Table, serde_yaml::Error> {
    let table: mapping::Table = serde_yaml::from_str(src)?;
    let columns: Vec<_> = table
        .columns
        .iter()
        .map(|c| Column {
            name: c.name.clone(),
            dtype: match c.r#type.as_str() {
                "u64" => Type::U64,
                "string" => Type::String,
                _ => panic!("unexpected {:?}", c.r#type),
            },
            default: match (&c.default, &c.auto_increment) {
                (Some(default), false) => Some(Default::Data(string_to_data(default.clone()))),
                (None, true) => Some(Default::AutoIncrement),
                (None, false) => None,
                _ => panic!("default???"),
            },
        })
        .collect();
    let primary_key = table
        .primary_key
        .map(|name| columns.iter().position(|c| c.name == name).unwrap());
    Ok(Table {
        name: table.name,
        columns,
        primary_key,
    })
}

mod mapping {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct Table {
        pub name: String,
        pub columns: Vec<Column>,
        pub primary_key: Option<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct Column {
        pub name: String,
        pub r#type: String,
        #[serde(default)]
        pub default: Option<String>,
        #[serde(default)]
        pub auto_increment: bool,
    }
}
