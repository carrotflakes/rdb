use crate::data::Data;

#[derive(Debug, Clone)]
pub struct Select {
    pub sub_queries: Vec<(String, Select)>,
    pub source: SelectSource,
    pub process: Vec<ProcessItem>,
    pub post_process: Vec<PostProcessItem>,
}

#[derive(Debug, Clone)]
pub struct SelectSource {
    pub table_name: String,
    pub keys: Vec<String>,
    pub from: Option<Vec<Data>>,
    pub to: Option<Vec<Data>>,
}

#[derive(Debug, Clone)]
pub enum ProcessItem {
    Select {
        columns: Vec<(String, Expr)>,
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
pub enum Expr {
    Column(String),
    Data(Data),
}

#[derive(Debug, Clone)]
pub enum PostProcessItem {
    SortBy { column_name: String },
    Skip { num: usize },
    Limit { num: usize },
}

#[derive(Debug, Clone)]
pub enum Insert {
    Row {
        table_name: String,
        column_names: Vec<String>,
        values: Vec<Data>,
    },
    Select {
        table_name: String,
        select: Select,
    },
}

#[derive(Debug, Clone)]
pub struct Delete {
    pub table_name: String,
    pub source: SelectSource,
    // TODO: filter
}

#[derive(Debug, Clone)]
pub struct Update {
    pub table_name: String,
    // TODO
}
