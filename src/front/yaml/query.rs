use crate::{
    data::Data,
    front::yaml::{query::mapping::ProcessSelectColumn, string_to_data},
    query::{Delete, Expr, FilterItem, Insert, ProcessItem, Select, SelectSource},
};

pub fn parse_select_from_yaml(src: &str) -> Result<Select, serde_yaml::Error> {
    let select: mapping::Select = serde_yaml::from_str(src)?;
    Ok(map_select(select))
}

pub fn map_select(select: mapping::Select) -> Select {
    Select {
        sub_queries: vec![],
        source: map_select_source(select.source),
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
                mapping::ProcessItem::Filter(filter_item) => ProcessItem::Filter {
                    items: vec![map_filter_item(filter_item)],
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
                mapping::ProcessItem::Distinct(column_name) => {
                    ProcessItem::Distinct { column_name }
                }
                mapping::ProcessItem::AddColumn { name, expr } => ProcessItem::AddColumn {
                    column_name: name,
                    expr: map_expr(expr),
                },
            })
            .collect(),
        post_process: vec![],
    }
}

pub fn map_select_source(source: mapping::SelectSource) -> SelectSource {
    match source.iterate {
        mapping::SelectSourceIterate {
            over,
            from,
            to,
            just: None,
        } => SelectSource {
            table_name: source.table,
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
                table_name: source.table,
                keys: over,
                from: just.clone(),
                to: just,
            }
        }
        _ => {
            panic!("unexpected iterate")
        }
    }
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

pub fn parse_delete_from_yaml(src: &str) -> Result<Delete, serde_yaml::Error> {
    let delete: mapping::Delete = serde_yaml::from_str(src)?;
    Ok(Delete {
        source: map_select_source(delete.source),
        filter: vec![],
    })
}

fn map_filter_item(filter_item: mapping::FilterItem) -> FilterItem {
    match filter_item {
        mapping::FilterItem::Eq(left, right) => FilterItem::Eq(map_expr(left), map_expr(right)),
    }
}

fn map_expr(expr: mapping::Expr) -> Expr {
    match expr {
        mapping::Expr::Column(column_name) => Expr::Column(column_name),
        mapping::Expr::String(string) => Expr::Data(Data::String(string)),
        mapping::Expr::U64(u64) => Expr::Data(Data::U64(u64)),
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
        Filter(FilterItem),
        Join {
            table: String,
            left_key: String,
            right_key: String,
        },
        Distinct(String),
        AddColumn {
            name: String,
            expr: Expr,
        },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum FilterItem {
        Eq(Expr, Expr),
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Expr {
        Column(String),
        String(String),
        U64(u64),
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

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct Delete {
        pub source: SelectSource,
    }
}
