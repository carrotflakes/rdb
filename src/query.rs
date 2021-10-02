use crate::data::Data;

#[derive(Debug, Clone)]
pub struct Query {
    pub sub_queries: Vec<(String, Query)>,
    pub source: QuerySource,
    pub process: Vec<ProcessItem>,
    pub post_process: Vec<PostProcessItem>,
}

#[derive(Debug, Clone)]
pub struct QuerySource {
    pub table_name: String,
    pub keys: Vec<String>,
    pub from: Option<Vec<Data>>,
    pub to: Option<Vec<Data>>,
}

#[derive(Debug, Clone)]
pub enum ProcessItem {
    Select {
        columns: Vec<(String, String)>,
    },
    Filter {
        left_key: String,
        right_key: String, // todo
    },
    Join {
        table_name: String,
        left_key: String,
        right_key: String,
    },
    Distinct {
        column_name: String,
    },
    AddColumn {
        hoge: String,
    },
    Skip {
        num: usize,
    },
    Limit {
        num: usize,
    },
}

#[derive(Debug, Clone)]
pub enum PostProcessItem {
    SortBy { column_name: String },
    Skip { num: usize },
    Limit { num: usize },
}
