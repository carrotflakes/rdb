use serde::Deserialize;

use crate::{
    data::Data,
    front::yaml::{query::mapping::ProcessSelectColumn, string_to_data},
    query::{
        Delete, Expr, FilterItem, Insert, PostProcessItem, ProcessItem, Query, Select,
        SelectSource, SelectSourceTable, Stream, Update,
    },
};

pub fn parse_named_queries_from_yaml(src: &str) -> Result<Vec<(String, Query)>, serde_yaml::Error> {
    serde_yaml::Deserializer::from_str(src)
        .map(|de| {
            mapping::NamedQuery::deserialize(de).and_then(|named_query| {
                map_query(named_query.query.clone()).map(|q| (named_query.name, q))
            })
        })
        .collect()
}

pub fn parse_query_from_yaml(src: &str) -> Result<Query, serde_yaml::Error> {
    let query: mapping::Query = serde_yaml::from_str(src)?;
    map_query(query)
}

pub fn parse_select_from_yaml(src: &str) -> Result<Select, serde_yaml::Error> {
    let select: mapping::Select = serde_yaml::from_str(src)?;
    map_select(select)
}

pub fn parse_insert_from_yaml(src: &str) -> Result<Insert, serde_yaml::Error> {
    let insert: mapping::Insert = serde_yaml::from_str(src)?;
    map_insert(insert)
}

pub fn parse_delete_from_yaml(src: &str) -> Result<Delete, serde_yaml::Error> {
    let delete: mapping::Delete = serde_yaml::from_str(src)?;
    map_delete(delete)
}

pub fn parse_update_from_yaml(src: &str) -> Result<Update, serde_yaml::Error> {
    let update: mapping::Update = serde_yaml::from_str(src)?;
    map_update(update)
}

fn map_query(query: mapping::Query) -> Result<Query, serde_yaml::Error> {
    Ok(match query {
        mapping::Query::Select(select) => Query::Select(map_select(select)?),
        mapping::Query::Insert(insert) => Query::Insert(map_insert(insert)?),
        mapping::Query::Delete(delete) => Query::Delete(map_delete(delete)?),
        mapping::Query::Update(update) => Query::Update(map_update(update)?),
    })
}

fn map_select(select: mapping::Select) -> Result<Select, serde_yaml::Error> {
    let post_process = select
        .post_process
        .iter()
        .cloned()
        .map(map_post_process_item)
        .collect::<Result<_, _>>()?;
    let streams = match select {
        mapping::Select {
            source: None,
            process,
            streams: Some(streams),
            ..
        } => streams
            .into_iter()
            .map(|s| {
                Ok(Stream {
                    source: map_select_source(s.source),
                    process: s
                        .process
                        .into_iter()
                        .chain(process.iter().cloned())
                        .map(map_process_item)
                        .collect::<Result<Vec<_>, serde_yaml::Error>>()?,
                })
            })
            .collect::<Result<Vec<_>, serde_yaml::Error>>()?,
        mapping::Select {
            source: Some(source),
            process,
            streams: None,
            ..
        } => vec![Stream {
            source: map_select_source(source),
            process: process
                .into_iter()
                .map(map_process_item)
                .collect::<Result<_, _>>()?,
        }],
        _ => panic!("invalid select"),
    };
    Ok(Select {
        sub_queries: vec![],
        streams,
        post_process,
    })
}

fn map_select_source(source: mapping::SelectSource) -> SelectSource {
    match source {
        mapping::SelectSource {
            table: Some(table),
            iterate: Some(iterate),
            iota: None,
        } => match iterate {
            mapping::SelectSourceIterate {
                over,
                from,
                to,
                just: None,
            } => SelectSource::Table(SelectSourceTable {
                table_name: table,
                keys: over,
                from: from.map(|x| x.into_iter().map(string_to_data).collect()),
                to: to.map(|x| x.into_iter().map(string_to_data).collect()),
            }),
            mapping::SelectSourceIterate {
                over,
                from: None,
                to: None,
                just: Some(just),
            } => {
                let just = Some(just.into_iter().map(string_to_data).collect());
                SelectSource::Table(SelectSourceTable {
                    table_name: table,
                    keys: over,
                    from: just.clone(),
                    to: just,
                })
            }
            _ => {
                panic!("unexpected iterate")
            }
        },
        mapping::SelectSource {
            table: None,
            iterate: None,
            iota: Some(iota),
        } => SelectSource::Iota {
            column_name: iota.column,
            from: iota.from,
            to: iota.to,
        },
        _ => panic!("invalid source"),
    }
}

fn iterate_from_to(
    select_source_iterate: mapping::SelectSourceIterate,
) -> (Option<Vec<Data>>, Option<Vec<Data>>) {
    match select_source_iterate {
        mapping::SelectSourceIterate {
            over: _,
            from,
            to,
            just: None,
        } => (
            from.map(|x| x.into_iter().map(string_to_data).collect()),
            to.map(|x| x.into_iter().map(string_to_data).collect()),
        ),
        mapping::SelectSourceIterate {
            over: _,
            from: None,
            to: None,
            just: Some(just),
        } => {
            let just = Some(just.into_iter().map(string_to_data).collect());
            (just.clone(), just)
        }
        _ => {
            panic!("unexpected iterate")
        }
    }
}

fn map_insert(insert: mapping::Insert) -> Result<Insert, serde_yaml::Error> {
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
            select: map_select(select)?,
        }),
        _ => panic!("row or select"),
    }
}

fn map_process_item(process_item: mapping::ProcessItem) -> Result<ProcessItem, serde_yaml::Error> {
    Ok(match process_item {
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
                        panic!("invalid select column: {:?}", x)
                    }
                })
                .collect(),
        },
        mapping::ProcessItem::Filter(filter_item) => ProcessItem::Filter {
            items: vec![map_filter_item(filter_item)],
        },
        mapping::ProcessItem::Join {
            table,
            left_keys,
            right_keys,
        } => ProcessItem::Join {
            table_name: table,
            left_keys,
            right_keys,
        },
        mapping::ProcessItem::Distinct(column_names) => ProcessItem::Distinct { column_names },
        mapping::ProcessItem::AddColumn { name, expr } => ProcessItem::AddColumn {
            column_name: name,
            expr: map_expr(expr),
        },
        mapping::ProcessItem::Skip(num) => ProcessItem::Skip { num },
        mapping::ProcessItem::Limit(num) => ProcessItem::Limit { num },
    })
}

fn map_post_process_item(
    post_process_item: mapping::PostProcessItem,
) -> Result<PostProcessItem, serde_yaml::Error> {
    Ok(match post_process_item {
        mapping::PostProcessItem::SortBy { column, order } => PostProcessItem::SortBy {
            column_name: column,
            ascent: matches!(order, mapping::SortOrder::Asc),
        },
        mapping::PostProcessItem::Skip { num } => PostProcessItem::Skip { num },
        mapping::PostProcessItem::Limit { num } => PostProcessItem::Limit { num },
    })
}

fn map_delete(delete: mapping::Delete) -> Result<Delete, serde_yaml::Error> {
    Ok(Delete {
        source: map_select_source(delete.source),
        filter: vec![],
    })
}

fn map_update(update: mapping::Update) -> Result<Update, serde_yaml::Error> {
    let column_names = update.columns.iter().map(|x| x.0.clone()).collect();
    let exprs = update
        .columns
        .iter()
        .map(|x| map_expr(x.1.clone()))
        .collect();
    Ok(Update {
        source: {
            let (from, to) = iterate_from_to(update.iterate.clone());
            SelectSourceTable {
                table_name: update.table,
                keys: update.iterate.over,
                from,
                to,
            }
        },
        filter_items: update.filter.into_iter().map(map_filter_item).collect(),
        column_names,
        exprs,
    })
}

fn map_filter_item(filter_item: mapping::FilterItem) -> FilterItem {
    match filter_item {
        mapping::FilterItem::Eq(left, right) => FilterItem::Eq(map_expr(left), map_expr(right)),
        mapping::FilterItem::Ne(left, right) => FilterItem::Ne(map_expr(left), map_expr(right)),
        mapping::FilterItem::Lt(left, right) => FilterItem::Lt(map_expr(left), map_expr(right)),
        mapping::FilterItem::Le(left, right) => FilterItem::Le(map_expr(left), map_expr(right)),
        mapping::FilterItem::Gt(left, right) => FilterItem::Gt(map_expr(left), map_expr(right)),
        mapping::FilterItem::Ge(left, right) => FilterItem::Ge(map_expr(left), map_expr(right)),
        mapping::FilterItem::And(left, right) => FilterItem::And(
            Box::new(map_filter_item(*left)),
            Box::new(map_filter_item(*right)),
        ),
        mapping::FilterItem::Or(left, right) => FilterItem::Or(
            Box::new(map_filter_item(*left)),
            Box::new(map_filter_item(*right)),
        ),
    }
}

fn map_expr(expr: mapping::Expr) -> Expr {
    match expr {
        mapping::Expr::Column(column_name) => Expr::Column(column_name),
        mapping::Expr::String(string) => Expr::Data(Data::String(string)),
        mapping::Expr::U64(u64) => Expr::Data(Data::U64(u64)),
        mapping::Expr::Enumerate(v) => Expr::Enumerate(Data::U64(v)),
    }
}

mod mapping {
    use std::collections::{BTreeMap, HashMap};

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct NamedQuery {
        pub name: String,
        #[serde(flatten)]
        pub query: Query,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Query {
        Select(Select),
        Insert(Insert),
        Delete(Delete),
        Update(Update),
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct Select {
        #[serde(default)]
        pub source: Option<SelectSource>,
        #[serde(default)]
        pub process: Vec<ProcessItem>,
        #[serde(default)]
        pub streams: Option<Vec<Stream>>,
        #[serde(default)]
        pub post_process: Vec<PostProcessItem>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct Stream {
        pub source: SelectSource,
        #[serde(default)]
        pub process: Vec<ProcessItem>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct SelectSource {
        pub table: Option<String>,
        pub iterate: Option<SelectSourceIterate>,
        pub iota: Option<Iota>,
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
    pub struct Iota {
        pub column: String,
        pub from: u64,
        pub to: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum ProcessItem {
        Select(Vec<ProcessSelectColumn>),
        Filter(FilterItem),
        Join {
            table: String,
            left_keys: Vec<String>,
            right_keys: Vec<String>,
        },
        Distinct(Vec<String>),
        AddColumn {
            name: String,
            expr: Expr,
        },
        Skip(usize),
        Limit(usize),
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum FilterItem {
        Eq(Expr, Expr),
        Ne(Expr, Expr),
        Lt(Expr, Expr),
        Le(Expr, Expr),
        Gt(Expr, Expr),
        Ge(Expr, Expr),
        And(Box<FilterItem>, Box<FilterItem>),
        Or(Box<FilterItem>, Box<FilterItem>),
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Expr {
        Column(String),
        String(String),
        U64(u64),
        Enumerate(u64),
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
        SortBy { column: String, order: SortOrder },
        Skip { num: usize },
        Limit { num: usize },
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum SortOrder {
        Asc,
        Desc,
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

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub struct Update {
        pub table: String,
        pub iterate: SelectSourceIterate,
        #[serde(default)]
        pub filter: Vec<FilterItem>,
        pub columns: BTreeMap<String, Expr>,
    }
}
