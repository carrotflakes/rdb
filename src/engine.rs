use std::collections::HashSet;

use crate::{
    data::Data,
    query::{self, ProcessItem, Query, Select, SelectSource, Stream},
    schema::Schema,
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

        let columns = self.stream_columns(&select.streams[0]);
        for stream in select.streams.iter().skip(1) {
            if self.stream_columns(stream) != columns {
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

    pub fn execute_delete(&mut self, delete: &query::Delete) -> Result<(), String> {
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
                // self.storage.cursor_advance(&mut cursor);
            } else {
                break;
            }
        }
        Ok(())
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
        let (_, table) = self.schema().get_table(table_name).unwrap();
        let columns = table.columns.clone();
        let values = columns
            .iter()
            .map(|column| {
                if let Some(i) = column_names.iter().position(|n| &column.name == n) {
                    values[i].clone()
                } else if let Some(default) = &column.default {
                    match default {
                        crate::schema::Default::Data(d) => d.clone(),
                        crate::schema::Default::AutoIncrement => {
                            Data::U64(self.storage.issue_auto_increment(table_name, &column.name))
                        }
                    }
                } else {
                    panic!("no default")
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

    fn stream_columns(&self, stream: &Stream) -> Vec<String> {
        let mut columns = match &stream.source {
            SelectSource::Table(source_table) => {
                let table =
                    if let Some((_, table)) = self.schema().get_table(&source_table.table_name) {
                        table
                    } else {
                        panic!("missing table");
                    };
                table.columns.iter().map(|c| c.name.to_owned()).collect()
            }
            SelectSource::Iota {
                column_name,
                from,
                to,
            } => vec![column_name.clone()],
        };

        for p in &stream.process {
            columns = select_process_item_column(self.schema(), p, &columns);
        }

        columns
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
                let (_, mut appender) = build_excecutable_query_process(
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
                let (_, mut appender) = build_excecutable_query_process(
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
}

pub struct QueryContext<'a, S: Storage> {
    storage: &'a S,
    ended: bool,
}

type RowAppender<S> = Box<dyn for<'a> FnMut(&mut QueryContext<'a, S>, Vec<Data>)>;

fn build_excecutable_query_process<S: Storage>(
    schema: &Schema,
    storage: &S,
    columns: Vec<String>,
    process: &[ProcessItem],
    mut appender: RowAppender<S>,
) -> (Vec<String>, RowAppender<S>) {
    let mut columnss = vec![columns];
    for p in process {
        let columns = columnss.last().unwrap();
        let columns = select_process_item_column(schema, p, columns);
        columnss.push(columns);
    }

    for (p, pre_post_columns) in process.iter().zip(columnss.windows(2)).rev() {
        let pre_columns = &pre_post_columns[0];
        let post_columns = &pre_post_columns[1];
        appender = process_item_appender(p, appender, pre_columns, post_columns, schema, storage);
    }

    (columnss.pop().unwrap(), appender)
}

impl<S: Storage> Engine<S> {
    pub fn flush(&self) {
        self.storage.flush();
    }
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
            enum Item {
                Eq(Expr, Expr),
            }
            let mut items = items
                .iter()
                .map(|item| match item {
                    query::FilterItem::Eq(left, right) => {
                        Item::Eq(convert_expr(left), convert_expr(right))
                    }
                })
                .collect::<Vec<_>>();
            Box::new(move |ctx, row| {
                for item in &mut items {
                    match item {
                        Item::Eq(left, right) => {
                            if left.eval(&row) != right.eval(&row) {
                                return;
                            }
                        }
                    }
                }
                appender(ctx, row);
            })
        }
        ProcessItem::Join {
            table_name,
            left_key,
            right_key,
        } => {
            let left_i = post_columns.iter().position(|c| c == left_key).unwrap();
            let (_, table) = schema.get_table(table_name).unwrap();
            let right_i = table
                .columns
                .iter()
                .position(|c| &c.name == right_key)
                .unwrap();
            let source_index = storage
                .source_index(table_name, &[right_key.clone()])
                .unwrap(); // TODO!
            Box::new(move |ctx, row| {
                let mut cursor = ctx
                    .storage
                    .get_cursor_just(source_index, &vec![row[left_i].clone()]);
                if ctx.storage.cursor_is_end(&cursor) {
                    return;
                }
                while {
                    !ctx.storage.cursor_is_end(&cursor)
                        && if let Some(append_row) = ctx.storage.cursor_get_row(&cursor) {
                            if append_row[right_i] == row[left_i] {
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
        ProcessItem::Distinct { column_name } => {
            let column_index = pre_columns.iter().position(|x| x == column_name).unwrap();
            let mut hashset = HashSet::new();

            Box::new(move |ctx, row| {
                if hashset.insert(row[column_index].clone()) {
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
