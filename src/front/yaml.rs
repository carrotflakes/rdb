use crate::{
    data::Data,
    query::{ProcessItem, Query, QuerySource},
};

pub fn parse_query_from_yaml(src: &str) -> Result<Query, serde_yaml::Error> {
    let query: mapping::Query = serde_yaml::from_str(src)?;
    Ok(Query {
        sub_queries: vec![],
        source: match query.source.iterate {
            mapping::QuerySourceIterate {
                over,
                from,
                to,
                just: None,
            } => QuerySource {
                table_name: query.source.table,
                keys: over,
                from: from
                    .map(|x| x.into_iter().map(string_to_data).collect()),
                to: to
                    .map(|x| x.into_iter().map(string_to_data).collect()),
            },
            mapping::QuerySourceIterate {
                over,
                from: None,
                to: None,
                just: Some(just),
            } => {
                let just = Some(just.into_iter().map(string_to_data).collect());
                QuerySource {
                table_name: query.source.table,
                keys: over,
                from: just.clone(),
                to: just,
            }},
            _ => {
                panic!("unexpected iterate")
            }
        } ,
        process: query
            .process
            .into_iter()
            .map(|x| match x {
                mapping::ProcessItem::Select(columns) => ProcessItem::Select {
                    columns: columns.into_iter().map(|x| (x.name, x.r#as)).collect(),
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
    })
}

pub fn string_to_data(str: String) -> Data {
    if let Ok(v) = str.parse() {
        Data::U64(v)
    } else {
        Data::String(str.clone())
    }
}

pub fn query_to_yaml(query: &Query) -> String {
    serde_yaml::to_string(&mapping::Query {
        source: mapping::QuerySource {
            table: query.source.table_name.clone(),
            iterate: mapping::QuerySourceIterate {
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

mod mapping {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct Query {
        pub source: QuerySource,
        #[serde(default)]
        pub process: Vec<ProcessItem>,
        #[serde(default)]
        pub post_process: Vec<PostProcessItem>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct QuerySource {
        pub table: String,
        pub iterate: QuerySourceIterate,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct QuerySourceIterate {
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
        pub name: String,
        pub r#as: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum PostProcessItem {
        SortBy { column_name: String },
        Skip { num: usize },
        Limit { num: usize },
    }
}
