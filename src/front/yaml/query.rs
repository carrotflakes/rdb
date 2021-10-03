use crate::{
    front::yaml::{query::mapping::ProcessSelectColumn, string_to_data},
    query::{Expr, Insert, ProcessItem, Select, SelectSource},
};

pub fn parse_select_from_yaml(src: &str) -> Result<Select, serde_yaml::Error> {
    let select: mapping::Select = serde_yaml::from_str(src)?;
    Ok(map_select(select))
}

pub fn map_select(select: mapping::Select) -> Select {
    Select {
        sub_queries: vec![],
        source: match select.source.iterate {
            mapping::SelectSourceIterate {
                over,
                from,
                to,
                just: None,
            } => SelectSource {
                table_name: select.source.table,
                keys: over,
                from: from.map(|x| x.into_iter().map(string_to_data).collect()),
                to: to.map(|x| x.into_iter().map(string_to_data).collect()),
            },
            mapping::SelectSourceIterate {
                over,
                from: None,
                to: None,
                just: Some(just),
            } => {
                let just = Some(just.into_iter().map(string_to_data).collect());
                SelectSource {
                    table_name: select.source.table,
                    keys: over,
                    from: just.clone(),
                    to: just,
                }
            }
            _ => {
                panic!("unexpected iterate")
            }
        },
        process: select
            .process
            .into_iter()
            .map(|x| match x {
                mapping::ProcessItem::Select(columns) => ProcessItem::Select {
                    columns: columns
                        .into_iter()
                        .map(|x| match x {
                            ProcessSelectColumn {
                                name: Some(name),
                                from: Some(from),
                                value: None,
                            } => (name, Expr::Column(from)),
                            ProcessSelectColumn {
                                name: Some(name),
                                from: None,
                                value: Some(value),
                            } => (name, Expr::Data(string_to_data(value))),
                            ProcessSelectColumn {
                                name: Some(name),
                                from: None,
                                value: None,
                            } => (name.clone(), Expr::Column(name)),
                            _ => {
                                panic!("oops")
                            }
                        })
                        .collect(),
                },
                mapping::ProcessItem::Filter {
                    left_key,
                    right_key,
                } => ProcessItem::Filter {
                    left_key,
                    right_key,
                },
                mapping::ProcessItem::Join {
                    table,
                    left_key,
                    right_key,
                } => ProcessItem::Join {
                    table_name: table,
                    left_key,
                    right_key,
                },
            })
            .collect(),
        post_process: vec![],
    }
}

pub fn query_to_yaml(query: &Select) -> String {
    serde_yaml::to_string(&mapping::Select {
        source: mapping::SelectSource {
            table: query.source.table_name.clone(),
            iterate: mapping::SelectSourceIterate {
                over: query.source.keys.clone(),
                from: None,
                to: None,
                just: None,
            },
        },
        process: todo!(),
        post_process: vec![],
    })
    .unwrap()
}

pub fn parse_insert_from_yaml(src: &str) -> Result<Insert, serde_yaml::Error> {
    let insert: mapping::Insert = serde_yaml::from_str(src)?;
    match insert {
        mapping::Insert {
            table,
            row: Some(row),
            select: None,
        } => Ok(Insert::Row {
            table_name: table,
            column_names: row.keys().map(|s| s.to_owned()).collect(),
            values: row
                .values()
                .map(|s| s.to_owned())
                .map(string_to_data)
                .collect(),
        }),
        mapping::Insert {
            table,
            row: None,
            select: Some(select),
        } => Ok(Insert::Select {
            table_name: table,
            select: map_select(select),
        }),
        _ => panic!("row or select"),
    }
}

mod mapping {
    use std::collections::HashMap;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct Select {
        pub source: SelectSource,
        #[serde(default)]
        pub process: Vec<ProcessItem>,
        #[serde(default)]
        pub post_process: Vec<PostProcessItem>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct SelectSource {
        pub table: String,
        pub iterate: SelectSourceIterate,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct SelectSourceIterate {
        pub over: Vec<String>,
        pub from: Option<Vec<String>>,
        pub to: Option<Vec<String>>,
        pub just: Option<Vec<String>>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ProcessItem {
        Select(Vec<ProcessSelectColumn>),
        Filter {
            left_key: String,
            right_key: String, // todo
        },
        Join {
            table: String,
            left_key: String,
            right_key: String,
        },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct ProcessSelectColumn {
        pub name: Option<String>,
        pub from: Option<String>,
        pub value: Option<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum PostProcessItem {
        SortBy { column_name: String },
        Skip { num: usize },
        Limit { num: usize },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct Insert {
        pub table: String,
        pub row: Option<HashMap<String, String>>,
        pub select: Option<Select>,
    }
}
