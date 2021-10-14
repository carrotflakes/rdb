use std::collections::HashSet;

use crate::{
    data::Data,
    query::{self, ProcessItem, Query, Select, SelectSource, SelectSourceTable, Stream},
    schema::{self, Schema},
    storage::Storage,
};

pub struct Engine<S: Storage> {
    storage: S,
}

impl<S: Storage> Engine<S> {
    pub fn from_storage(storage: S) -> Self {
        Self { storage }
    }

    pub fn schema(&self) -> &Schema {
        self.storage.schema()
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn create_table(&mut self, table: schema::Table) {
        self.storage.add_table(table);
    }

    pub fn execute_query(&mut self, query: &Query) -> Result<(Vec<String>, Vec<Data>), String> {
        match query {
            Query::Select(select) => self.execute_select(select),
            Query::Insert(insert) => self.execute_insert(insert).map(|_| (vec![], vec![])),
            Query::Delete(delete) => self.execute_delete(delete).map(|_| (vec![], vec![])),
            Query::Update(update) => self.execute_update(update).map(|_| (vec![], vec![])),
        }
    }

    pub fn execute_select(&self, select: &Select) -> Result<(Vec<String>, Vec<Data>), String> {
        let mut rows = vec![];

        let columns = stream_columns(self.schema(), &select.streams[0]);
        for stream in select.streams.iter().skip(1) {
            if stream_columns(self.schema(), stream) != columns {
                return Err(format!("streams have different columns"));
            }
        }

        for stream in select.streams.iter() {
            let appender: RowAppender<_> = {
                let rows =
                    unsafe { std::mem::transmute::<*mut Vec<Data>, &mut Vec<Data>>(&mut rows) };
                Box::new(move |_, row| {
                    rows.extend(row);
                })
            };

            self.scan(stream, appender)?;
        }

        Ok((columns, rows))
    }

    pub fn execute_insert(&mut self, insert: &query::Insert) -> Result<(), String> {
        match insert {
            query::Insert::Row {
                table_name,
                column_names,
                values,
            } => self.execute_insert_row(table_name, column_names, values),
            query::Insert::Select { table_name, select } => {
                self.execute_insert_from_select(table_name, select)
            }
        }
    }

    pub fn execute_delete(&mut self, delete: &query::Delete) -> Result<usize, String> {
        let source_table = if let SelectSource::Table(table) = &delete.source {
            table
        } else {
            return Err(format!("delete supports only to table"));
        };
        let table = if let Some((_, table)) = self.schema().get_table(&source_table.table_name) {
            table
        } else {
            return Err(format!("missing table"));
        };
        let source = self
            .storage
            .source_index(&source_table.table_name, &source_table.keys)
            .unwrap();
        let mut cursor = if let Some(from) = &source_table.from {
            self.storage.get_cursor_just(source, from)
        } else {
            self.storage.get_cursor_first(source)
        };
        let end_check_columns = source_table.to.as_ref().map(|to| {
            (
                source_table
                    .keys
                    .iter()
                    .map(|name| table.get_column(name).unwrap().0)
                    .collect::<Vec<_>>(),
                to.clone(),
            )
        });
        let mut count = 0;
        while !self.storage.cursor_is_end(&cursor) {
            if let Some(row) = self.storage.cursor_get_row(&cursor) {
                if let Some((cs, to)) = &end_check_columns {
                    let now = cs.iter().map(|i| row[*i].clone()).collect::<Vec<_>>();
                    if to < &now {
                        break;
                    }
                }
                // todo: filter here
                self.storage.cursor_delete(&mut cursor);
                self.storage.cursor_next_occupied(&mut cursor);
                count += 1;
                // self.storage.cursor_advance(&mut cursor);
            } else {
                break;
            }
        }
        Ok(count)
    }

    pub fn execute_update(&mut self, update: &query::Update) -> Result<(), String> {
        let table = if let Some((_, table)) = self.schema().get_table(&update.source.table_name) {
            table.clone()
        } else {
            return Err(format!("missing table"));
        };

        let source = self
            .storage
            .source_index(&table.name, &update.source.keys)
            .unwrap();
        let mut cursor = if let Some(from) = &update.source.from {
            self.storage.get_cursor_just(source, from)
        } else {
            self.storage.get_cursor_first(source)
        };
        let end_check_columns = update.source.to.as_ref().map(|to| {
            (
                update
                    .source
                    .keys
                    .iter()
                    .map(|name| table.get_column(name).unwrap().0)
                    .collect::<Vec<_>>(),
                to.clone(),
            )
        });

        let mut rows = vec![];
        while !self.storage.cursor_is_end(&cursor) {
            if let Some(row) = self.storage.cursor_get_row(&cursor) {
                if let Some((cs, to)) = &end_check_columns {
                    let now = cs.iter().map(|i| row[*i].clone()).collect::<Vec<_>>();
                    if to < &now {
                        break;
                    }
                }
                rows.push(row);
                self.storage.cursor_delete(&mut cursor);
                self.storage.cursor_next_occupied(&mut cursor);
            } else {
                break;
            }
        }

        let mut exprs: Vec<_> = table
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if let Some(i) = update.column_names.iter().position(|n| n == &c.name) {
                    match &update.exprs[i] {
                        query::Expr::Column(name) => Expr::Column(
                            update.column_names.iter().position(|n| n == name).unwrap(),
                        ),
                        query::Expr::Data(d) => Expr::Data(d.clone()),
                        query::Expr::Enumerate(_) => todo!(),
                    }
                } else {
                    Expr::Column(i)
                }
            })
            .collect();

        for row in rows {
            let row = exprs.iter_mut().map(|e| e.eval(&row)).collect();
            self.storage.add_row(&table.name, row)?;
        }

        Ok(())
    }

    fn execute_insert_row(
        &mut self,
        table_name: &String,
        column_names: &Vec<String>,
        values: &[Data],
    ) -> Result<(), String> {
        let (_, table) = self
            .schema()
            .get_table(table_name)
            .expect("table not found");
        let columns = table.columns.clone();
        let values = columns
            .iter()
            .map(|column| {
                if let Some(i) = column_names.iter().position(|n| &column.name == n) {
                    if let Some(crate::schema::Default::AutoIncrement) = &column.default {
                        self.update_auto_inc(&table_name, &column_names[i], &values[i]);
                    }
                    values[i].clone()
                } else {
                    match column.default.as_ref().expect("no default") {
                        crate::schema::Default::Data(d) => d.clone(),
                        crate::schema::Default::AutoIncrement => {
                            self.auto_inc(table_name, &column.name)
                        }
                    }
                }
            })
            .collect();
        self.storage.add_row(&table_name, values)
    }

    fn execute_insert_from_select(
        &mut self,
        table_name: &String,
        select: &Select,
    ) -> Result<(), String> {
        let (columns, rows) = self.execute_select(select)?;
        for row in rows.chunks(columns.len()) {
            self.execute_insert_row(table_name, &columns, row)?;
        }
        Ok(())
    }

    fn scan(&self, stream: &Stream, appender: RowAppender<S>) -> Result<(), String> {
        match &stream.source {
            SelectSource::Table(source_table) => {
                let table =
                    if let Some((_, table)) = self.schema().get_table(&source_table.table_name) {
                        table
                    } else {
                        return Err(format!("missing table"));
                    };
                let columns = table.columns.iter().map(|c| c.name.to_owned()).collect();
                let mut appender = build_excecutable_query_process(
                    self.schema(),
                    &self.storage,
                    columns,
                    &stream.process,
                    appender,
                );

                let source = self
                    .storage
                    .source_index(&table.name, &source_table.keys)
                    .unwrap();
                let mut cursor = if let Some(from) = &source_table.from {
                    self.storage.get_cursor_just(source, from)
                } else {
                    self.storage.get_cursor_first(source)
                };
                self.storage.cursor_next_occupied(&mut cursor); // get_cursor_justでページの最後を示すカーソルが返ってくる可能性がある
                let end_check_columns = source_table.to.as_ref().map(|to| {
                    (
                        source_table
                            .keys
                            .iter()
                            .map(|name| table.get_column(name).unwrap().0)
                            .collect::<Vec<_>>(),
                        to.clone(),
                    )
                });
                let mut ctx = QueryContext {
                    storage: &self.storage,
                    ended: false,
                };
                while !ctx.ended && !self.storage.cursor_is_end(&cursor) {
                    if let Some(row) = self.storage.cursor_get_row(&cursor) {
                        if let Some((cs, to)) = &end_check_columns {
                            let now = cs.iter().map(|i| row[*i].clone()).collect::<Vec<_>>();
                            if to < &now {
                                break;
                            }
                        }
                        appender(&mut ctx, row);
                        self.storage.cursor_advance(&mut cursor);
                    } else {
                        break;
                    }
                }
            }
            SelectSource::Iota {
                column_name,
                from,
                to,
            } => {
                let mut appender = build_excecutable_query_process(
                    self.schema(),
                    &self.storage,
                    vec![column_name.clone()],
                    &stream.process,
                    appender,
                );

                let mut ctx = QueryContext {
                    storage: &self.storage,
                    ended: false,
                };
                for i in *from..*to {
                    appender(&mut ctx, vec![Data::U64(i)]);
                }
            }
        }

        Ok(())
    }

    fn auto_inc(&mut self, table_name: &str, column_name: &str) -> Data {
        let select_source_table = SelectSourceTable {
            table_name: "auto_increment".to_owned(),
            keys: vec!["table".to_owned(), "column".to_owned()],
            from: Some(vec![
                Data::String(table_name.to_owned()),
                Data::String(column_name.to_owned()),
            ]),
            to: Some(vec![
                Data::String(table_name.to_owned()),
                Data::String(column_name.to_owned()),
            ]),
        };
        let (_, datas) = self
            .execute_select(&Select {
                sub_queries: vec![],
                streams: vec![Stream {
                    source: SelectSource::Table(select_source_table.clone()),
                    process: vec![ProcessItem::Select {
                        columns: vec![("num".to_owned(), query::Expr::Column("num".to_owned()))],
                    }],
                }],
                post_process: vec![],
            })
            .unwrap();
        if datas.is_empty() {
            self.execute_insert(&query::Insert::Row {
                table_name: "auto_increment".to_owned(),
                column_names: vec!["table".to_owned(), "column".to_owned(), "num".to_owned()],
                values: vec![
                    Data::String(table_name.to_owned()),
                    Data::String(column_name.to_owned()),
                    Data::U64(2),
                ],
            })
            .unwrap();
            Data::U64(1)
        } else {
            let data = datas[0].clone();
            self.execute_update(&query::Update {
                source: select_source_table,
                filter_items: vec![],
                column_names: vec!["num".to_owned()],
                exprs: vec![query::Expr::Data(match &data {
                    Data::U64(v) => Data::U64(v + 1),
                    _ => panic!(),
                })],
            })
            .unwrap();
            data
        }
    }

    fn update_auto_inc(&mut self, table_name: &str, column_name: &str, data: &Data) {
        let select_source_table = SelectSourceTable {
            table_name: "auto_increment".to_owned(),
            keys: vec!["table".to_owned(), "column".to_owned()],
            from: Some(vec![
                Data::String(table_name.to_owned()),
                Data::String(column_name.to_owned()),
            ]),
            to: Some(vec![
                Data::String(table_name.to_owned()),
                Data::String(column_name.to_owned()),
            ]),
        };

        let (_, datas) = self
            .execute_select(&Select {
                sub_queries: vec![],
                streams: vec![Stream {
                    source: SelectSource::Table(select_source_table.clone()),
                    process: vec![ProcessItem::Select {
                        columns: vec![("num".to_owned(), query::Expr::Column("num".to_owned()))],
                    }],
                }],
                post_process: vec![],
            })
            .unwrap();

        let insert_num = match &data {
            Data::U64(v) => *v,
            _ => panic!(),
        };

        if datas.is_empty() {
            self.execute_insert(&query::Insert::Row {
                table_name: "auto_increment".to_owned(),
                column_names: vec!["table".to_owned(), "column".to_owned(), "num".to_owned()],
                values: vec![
                    Data::String(table_name.to_owned()),
                    Data::String(column_name.to_owned()),
                    Data::U64(insert_num + 1),
                ],
            })
            .unwrap();
        } else {
            let ai_num = match &datas[0] {
                Data::U64(v) => *v,
                _ => panic!(),
            };

            if ai_num < insert_num {
                self.execute_update(&query::Update {
                    source: select_source_table,
                    filter_items: vec![],
                    column_names: vec!["num".to_owned()],
                    exprs: vec![query::Expr::Data(Data::U64(insert_num + 1))],
                })
                .unwrap();
            }
        }
    }
}

impl<S: Storage> Engine<S> {
    pub fn flush(&self) {
        self.storage.flush();
    }
}

pub struct QueryContext<'a, S: Storage> {
    storage: &'a S,
    ended: bool,
}

type RowAppender<S> = Box<dyn for<'a> FnMut(&mut QueryContext<'a, S>, Vec<Data>)>;

fn stream_columns(schema: &Schema, stream: &Stream) -> Vec<String> {
    let mut columns = match &stream.source {
        SelectSource::Table(source_table) => {
            let table = if let Some((_, table)) = schema.get_table(&source_table.table_name) {
                table
            } else {
                panic!("missing table");
            };
            table.columns.iter().map(|c| c.name.to_owned()).collect()
        }
        SelectSource::Iota { column_name, .. } => vec![column_name.clone()],
    };

    for p in &stream.process {
        columns = select_process_item_column(schema, p, &columns);
    }

    columns
}

fn build_excecutable_query_process<S: Storage>(
    schema: &Schema,
    storage: &S,
    columns: Vec<String>,
    process: &[ProcessItem],
    mut appender: RowAppender<S>,
) -> RowAppender<S> {
    let mut columns_vec = vec![columns];
    for p in process {
        let columns = columns_vec.last().unwrap();
        let columns = select_process_item_column(schema, p, columns);
        columns_vec.push(columns);
    }

    for (p, pre_post_columns) in process.iter().zip(columns_vec.windows(2)).rev() {
        let pre_columns = &pre_post_columns[0];
        let post_columns = &pre_post_columns[1];
        appender = process_item_appender(p, appender, pre_columns, post_columns, schema, storage);
    }

    appender
}

fn select_process_item_column(
    schema: &Schema,
    process_item: &ProcessItem,
    columns: &Vec<String>,
) -> Vec<String> {
    match process_item {
        ProcessItem::Select { columns: cs } => cs.iter().map(|x| x.0.to_string()).collect(),
        ProcessItem::Filter { .. } => columns.clone(),
        ProcessItem::Join { table_name, .. } => columns
            .iter()
            .cloned()
            .chain(
                schema
                    .get_table(table_name)
                    .unwrap()
                    .1
                    .columns
                    .iter()
                    .map(|c| format!("{}.{}", table_name, c.name)),
            )
            .collect(),
        ProcessItem::Distinct { .. } => columns.clone(),
        ProcessItem::AddColumn { column_name, .. } => {
            let mut columns = columns.clone();
            columns.push(column_name.clone());
            columns
        }
        ProcessItem::Skip { .. } => columns.clone(),
        ProcessItem::Limit { .. } => columns.clone(),
    }
}

fn process_item_appender<S: Storage>(
    p: &ProcessItem,
    mut appender: Box<dyn FnMut(&mut QueryContext<S>, Vec<Data>)>,
    pre_columns: &Vec<String>,
    post_columns: &Vec<String>,
    schema: &Schema,
    storage: &S,
) -> Box<dyn FnMut(&mut QueryContext<S>, Vec<Data>)> {
    let convert_expr = |expr: &query::Expr| -> Expr {
        match expr {
            query::Expr::Column(name) => {
                Expr::Column(pre_columns.iter().position(|x| x == name).unwrap())
            }
            query::Expr::Data(data) => Expr::Data(data.clone()),
            query::Expr::Enumerate(data) => Expr::Enumerate(data.clone()),
        }
    };

    match p {
        ProcessItem::Select { columns: cs } => {
            let mut exprs: Vec<_> = cs.iter().map(|x| convert_expr(&x.1)).collect();
            Box::new(move |ctx, row| {
                let row = exprs.iter_mut().map(|expr| expr.eval(&row)).collect();
                appender(ctx, row)
            })
        }
        ProcessItem::Filter { items } => {
            let mut items = items
                .iter()
                .map(|item| convert_filter_item(&convert_expr, item))
                .collect::<Vec<_>>();
            Box::new(move |ctx, row| {
                for item in &mut items {
                    if !item.eval(&row) {
                        return;
                    }
                }
                appender(ctx, row);
            })
        }
        ProcessItem::Join {
            table_name,
            left_keys,
            right_keys,
        } => {
            let left_is: Vec<_> = left_keys
                .iter()
                .map(|key| post_columns.iter().position(|c| c == key).unwrap())
                .collect();
            let (_, table) = schema.get_table(table_name).unwrap();
            let right_is: Vec<_> = right_keys
                .iter()
                .map(|key| table.columns.iter().position(|c| &c.name == key).unwrap())
                .collect();
            let source_index = storage
                .source_index(table_name, &right_keys)
                .expect(&format!("{:?} are not found in {}", right_keys, table_name)); // TODO!
            Box::new(move |ctx, row| {
                let mut cursor = ctx.storage.get_cursor_just(
                    source_index,
                    &left_is.iter().map(|i| row[*i].clone()).collect::<Vec<_>>(),
                );
                if ctx.storage.cursor_is_end(&cursor) {
                    return;
                }
                while {
                    !ctx.storage.cursor_is_end(&cursor)
                        && if let Some(append_row) = ctx.storage.cursor_get_row(&cursor) {
                            if left_is
                                .iter()
                                .zip(right_is.iter())
                                .all(|(left, right)| row[*left] == append_row[*right])
                            {
                                let mut row_ = row.clone();
                                row_.extend(append_row);
                                appender(ctx, row_);
                                ctx.storage.cursor_advance(&mut cursor)
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                } {}
            })
        }
        ProcessItem::Distinct { column_names } => {
            let column_indices: Vec<_> = column_names
                .iter()
                .map(|name| pre_columns.iter().position(|x| x == name).unwrap())
                .collect();
            let mut hashset = HashSet::<Vec<Data>>::new();

            Box::new(move |ctx, row| {
                if hashset.insert(column_indices.iter().map(|i| row[*i].clone()).collect()) {
                    appender(ctx, row);
                }
            })
        }
        ProcessItem::AddColumn { expr, .. } => {
            let mut expr = convert_expr(expr);
            Box::new(move |ctx, mut row| {
                let data = expr.eval(&row);
                row.push(data);
                appender(ctx, row);
            })
        }
        ProcessItem::Skip { num } => {
            let mut count = *num;
            Box::new(move |ctx, row| {
                if count == 0 {
                    appender(ctx, row);
                } else {
                    count -= 1;
                }
            })
        }
        ProcessItem::Limit { num } => {
            let mut count = *num;
            Box::new(move |ctx, row| {
                if count == 0 {
                    ctx.ended = true;
                } else {
                    appender(ctx, row);
                    count -= 1;
                }
            })
        }
    }
}

enum Item {
    Eq(Expr, Expr),
    Ne(Expr, Expr),
    Lt(Expr, Expr),
    Le(Expr, Expr),
    Gt(Expr, Expr),
    Ge(Expr, Expr),
    And(Box<Item>, Box<Item>),
    Or(Box<Item>, Box<Item>),
}

fn convert_filter_item(
    convert_expr: &impl Fn(&query::Expr) -> Expr,
    item: &query::FilterItem,
) -> Item {
    match item {
        query::FilterItem::Eq(left, right) => Item::Eq(convert_expr(left), convert_expr(right)),
        query::FilterItem::Ne(left, right) => Item::Ne(convert_expr(left), convert_expr(right)),
        query::FilterItem::Lt(left, right) => Item::Lt(convert_expr(left), convert_expr(right)),
        query::FilterItem::Le(left, right) => Item::Le(convert_expr(left), convert_expr(right)),
        query::FilterItem::Gt(left, right) => Item::Gt(convert_expr(left), convert_expr(right)),
        query::FilterItem::Ge(left, right) => Item::Ge(convert_expr(left), convert_expr(right)),
        query::FilterItem::And(left, right) => Item::And(
            Box::new(convert_filter_item(convert_expr, left)),
            Box::new(convert_filter_item(convert_expr, right)),
        ),
        query::FilterItem::Or(left, right) => Item::Or(
            Box::new(convert_filter_item(convert_expr, left)),
            Box::new(convert_filter_item(convert_expr, right)),
        ),
    }
}

impl Item {
    fn eval(&mut self, row: &[Data]) -> bool {
        match self {
            Item::Eq(left, right) => left.eval(&row) == right.eval(&row),
            Item::Ne(left, right) => left.eval(&row) != right.eval(&row),
            Item::Lt(left, right) => left.eval(&row) < right.eval(&row),
            Item::Le(left, right) => left.eval(&row) <= right.eval(&row),
            Item::Gt(left, right) => left.eval(&row) > right.eval(&row),
            Item::Ge(left, right) => left.eval(&row) >= right.eval(&row),
            Item::And(left, right) => left.eval(row) && right.eval(row),
            Item::Or(left, right) => left.eval(row) || right.eval(row),
        }
    }
}

enum Expr {
    Column(usize),
    Data(Data),
    Enumerate(Data),
}

impl Expr {
    fn eval(&mut self, row: &[Data]) -> Data {
        match self {
            Expr::Column(i) => row[*i].clone(),
            Expr::Data(d) => d.clone(),
            Expr::Enumerate(data) => {
                let ret = data.clone();
                match data {
                    Data::U64(v) => *v += 1,
                    Data::String(_) => panic!(),
                    Data::OptionU64(v) => {
                        if let Some(v) = v {
                            *v += 1;
                        }
                    }
                    Data::Lancer(size) => *size += 1,
                }
                ret
            }
        }
    }
}
