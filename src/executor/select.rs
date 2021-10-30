use crate::{data::Data, file::File, iterate::{Iterate}, query::{self, Select}, schema::Schema};

impl Select {
    pub async fn execute<I: Iterate>(&self, schema: &Schema, iterate: &I) -> Result<(Vec<String>, Vec<Data>), String> {

        let mut rows = vec![];

        let columns = stream_columns(schema, &self.streams[0]);
        for stream in self.streams.iter().skip(1) {
            if stream_columns(schema, stream) != columns {
                return Err(format!("streams have different columns"));
            }
        }

        for stream in self.streams.iter() {
            let appender: RowAppender<_> = {
                let rows =
                    unsafe { std::mem::transmute::<*mut Vec<Data>, &mut Vec<Data>>(&mut rows) };
                Box::new(move |_, row| {
                    rows.extend(row);
                })
            };

            self.scan(schema, iterate, stream, appender).await?;
        }

        todo!()
    }
    
    async fn scan<I: Iterate>(&self, schema: &Schema, iterate: &I, stream: &query::Stream, appender: RowAppender<I>) -> Result<(), String> {
        match &stream.source {
            query::SelectSource::Table(source_table) => {
                let table =
                    if let Some((_, table)) = schema.get_table(&source_table.table_name) {
                        table
                    } else {
                        return Err(format!("missing table"));
                    };
                let columns = table.columns.iter().map(|c| c.name.to_owned()).collect();
                let mut appender = build_excecutable_query_process(
                    schema,
                    iterate,
                    columns,
                    &stream.process,
                    appender,
                );

                let source = iterate
                    .iterate_index(&table.name, &source_table.keys)
                    .unwrap();
                let mut cursor = if let Some(from) = &source_table.from {
                    iterate.find(source, from)
                } else {
                    iterate.first_cursor(source)
                };
                iterate.cursor_next_occupied(&mut cursor); // get_cursor_justでページの最後を示すカーソルが返ってくる可能性がある
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
                    storage: iterate,
                    ended: false,
                };
                while !ctx.ended && !iterate.cursor_is_end(&cursor) {
                    if let Some(row) = iterate.cursor_get(&cursor) {
                        if let Some((cs, to)) = &end_check_columns {
                            let now = cs.iter().map(|i| row[*i].clone()).collect::<Vec<_>>();
                            if to < &now {
                                break;
                            }
                        }
                        appender(&mut ctx, row);
                        iterate.cursor_next(&mut cursor);
                    } else {
                        break;
                    }
                }
            }
            query::SelectSource::Iota {
                column_name,
                from,
                to,
            } => {
                let mut appender = build_excecutable_query_process(
                    schema,
                    iterate,
                    vec![column_name.clone()],
                    &stream.process,
                    appender,
                );

                let mut ctx = QueryContext {
                    storage: iterate,
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

pub struct QueryContext<'a, I: Iterate> {
    storage: &'a I,
    ended: bool,
}

type RowAppender<I> = Box<dyn for<'a> FnMut(&mut QueryContext<'a, I>, Vec<Data>)>;


fn stream_columns(schema: &Schema, stream: &query::Stream) -> Vec<String> {
    let mut columns = match &stream.source {
        query::SelectSource::Table(source_table) => {
            let table = if let Some((_, table)) = schema.get_table(&source_table.table_name) {
                table
            } else {
                panic!("missing table");
            };
            table.columns.iter().map(|c| c.name.to_owned()).collect()
        }
        query::SelectSource::Iota { column_name, .. } => vec![column_name.clone()],
    };

    for p in &stream.process {
        columns = select_process_item_column(schema, p, &columns);
    }

    columns
}

fn build_excecutable_query_process<I: Iterate>(
    schema: &Schema,
    storage: &I,
    columns: Vec<String>,
    process: &[query::ProcessItem],
    mut appender: RowAppender<I>,
) -> RowAppender<I> {
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
    process_item: &query::ProcessItem,
    columns: &Vec<String>,
) -> Vec<String> {
    match process_item {
        query::ProcessItem::Select { columns: cs } => cs.iter().map(|x| x.0.to_string()).collect(),
        query::ProcessItem::Filter { .. } => columns.clone(),
        query::ProcessItem::Join { table_name, .. } => columns
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
        query::ProcessItem::Distinct { .. } => columns.clone(),
        query::ProcessItem::AddColumn { column_name, .. } => {
            let mut columns = columns.clone();
            columns.push(column_name.clone());
            columns
        }
        query::ProcessItem::Skip { .. } => columns.clone(),
        query::ProcessItem::Limit { .. } => columns.clone(),
    }
}

fn process_item_appender<I: Iterate>(
    p: &query::ProcessItem,
    mut appender: Box<dyn FnMut(&mut QueryContext<I>, Vec<Data>)>,
    pre_columns: &Vec<String>,
    post_columns: &Vec<String>,
    schema: &Schema,
    storage: &I,
) -> Box<dyn FnMut(&mut QueryContext<I>, Vec<Data>)> {
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
        query::ProcessItem::Select { columns: cs } => {
            let mut exprs: Vec<_> = cs.iter().map(|x| convert_expr(&x.1)).collect();
            Box::new(move |ctx, row| {
                let row = exprs.iter_mut().map(|expr| expr.eval(&row)).collect();
                appender(ctx, row)
            })
        }
        query::ProcessItem::Filter { items } => {
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
        query::ProcessItem::Join {
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
                let mut cursor = ctx.storage.find(
                    source_index,
                    &left_is.iter().map(|i| row[*i].clone()).collect::<Vec<_>>(),
                );
                if ctx.storage.cursor_is_end(&cursor) {
                    return;
                }
                while {
                    !ctx.storage.cursor_is_end(&cursor)
                        && if let Some(append_row) = ctx.storage.cursor_get(&cursor) {
                            if left_is
                                .iter()
                                .zip(right_is.iter())
                                .all(|(left, right)| row[*left] == append_row[*right])
                            {
                                let mut row_ = row.clone();
                                row_.extend(append_row);
                                appender(ctx, row_);
                                ctx.storage.cursor_next(&mut cursor)
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                } {}
            })
        }
        query::ProcessItem::Distinct { column_names } => {
            let column_indices: Vec<_> = column_names
                .iter()
                .map(|name| pre_columns.iter().position(|x| x == name).unwrap())
                .collect();
            let mut hashset = std::collections::HashSet::<Vec<Data>>::new();

            Box::new(move |ctx, row| {
                if hashset.insert(column_indices.iter().map(|i| row[*i].clone()).collect()) {
                    appender(ctx, row);
                }
            })
        }
        query::ProcessItem::AddColumn { expr, .. } => {
            let mut expr = convert_expr(expr);
            Box::new(move |ctx, mut row| {
                let data = expr.eval(&row);
                row.push(data);
                appender(ctx, row);
            })
        }
        query::ProcessItem::Skip { num } => {
            let mut count = *num;
            Box::new(move |ctx, row| {
                if count == 0 {
                    appender(ctx, row);
                } else {
                    count -= 1;
                }
            })
        }
        query::ProcessItem::Limit { num } => {
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
