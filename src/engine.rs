use crate::{
    data::Data,
    query::{self, ProcessItem, Select},
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

    pub fn execute_select(&self, select: &Select) -> Result<(Vec<String>, Vec<Data>), String> {
        let mut rows = vec![];
        let appender: RowAppender<_> = {
            let rows = unsafe { std::mem::transmute::<*mut Vec<Data>, &mut Vec<Data>>(&mut rows) };
            Box::new(move |_, row| {
                rows.extend(row);
            })
        };

        let columns = self.scan(select, appender)?;

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

    pub fn execute_insert_row(
        &mut self,
        table_name: &String,
        column_names: &Vec<String>,
        values: &[Data],
    ) -> Result<(), String> {
        let columns = self
            .schema()
            .get_table(table_name)
            .unwrap()
            .1
            .columns
            .clone();
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

    pub fn execute_insert_from_select(
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

    fn scan(&self, select: &Select, appender: RowAppender<S>) -> Result<Vec<String>, String> {
        let table = if let Some((_, table)) = self.schema().get_table(&select.source.table_name) {
            table
        } else {
            return Err(format!("missing table"));
        };
        let columns = table.columns.iter().map(|c| c.name.to_owned()).collect();
        let (columns, mut appender) = build_excecutable_query_process(
            self.schema(),
            &self.storage,
            columns,
            &select.process,
            appender,
        );

        let source = self
            .storage
            .source_index(&table.name, &select.source.keys)
            .unwrap();
        let mut cursor = if let Some(from) = &select.source.from {
            self.storage.get_cursor_just(source, from)
        } else {
            self.storage.get_cursor_first(source)
        };
        let end_check_columns = select.source.to.as_ref().map(|to| {
            (
                select
                    .source
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
        Ok(columns)
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

        match p {
            ProcessItem::Select { columns: cs } => {
                enum Expr2 {
                    Column(usize),
                    Data(Data),
                }
                let exprs: Vec<_> = cs
                    .iter()
                    .map(|(_, expr)| match expr {
                        query::Expr::Column(c) => {
                            Expr2::Column(pre_columns.iter().position(|x| x == c).unwrap())
                        }
                        query::Expr::Data(d) => Expr2::Data(d.clone()),
                    })
                    .collect();
                appender = Box::new(move |ctx, row| {
                    let row = exprs
                        .iter()
                        .map(|e| match e {
                            Expr2::Column(i) => row[*i].clone(),
                            Expr2::Data(d) => d.clone(),
                        })
                        .collect();
                    appender(ctx, row)
                });
            }
            ProcessItem::Filter {
                left_key,
                right_key,
            } => {
                let left_i = pre_columns.iter().position(|c| c == left_key).unwrap();
                let right_i = pre_columns.iter().position(|c| c == right_key).unwrap();
                appender = Box::new(move |ctx, row| {
                    if row[left_i] == row[right_i] {
                        appender(ctx, row)
                    }
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
                appender = Box::new(move |ctx, row| {
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
                });
            }
            ProcessItem::Distinct { column_name } => todo!(),
            ProcessItem::AddColumn { hoge } => todo!(),
            ProcessItem::Skip { num } => todo!(),
            ProcessItem::Limit { num } => todo!(),
        }
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
        ProcessItem::Filter {
            left_key,
            right_key,
        } => columns.clone(),
        ProcessItem::Join {
            table_name,
            left_key,
            right_key,
        } => columns
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
        ProcessItem::Distinct { column_name } => columns.clone(),
        ProcessItem::AddColumn { hoge } => columns.clone(),
        ProcessItem::Skip { num } => columns.clone(),
        ProcessItem::Limit { num } => columns.clone(),
    }
}
