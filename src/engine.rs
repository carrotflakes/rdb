use crate::{
    data::Data,
    query::{self, ProcessItem, Select},
    schema::Schema,
    storage::Storage,
};

pub struct Engine<S: Storage> {
    schema: Schema,
    storage: S,
}

impl<S: Storage> Engine<S> {
    pub fn new(schema: Schema, storage: S) -> Self {
        Self { schema, storage }
    }

    pub fn execute_select(&self, query: &Select) -> Result<(Vec<String>, Vec<Data>), String> {
        let mut rows = vec![];
        let appender: RowAppender<_> = {
            let rows = unsafe { std::mem::transmute::<*mut Vec<Data>, &mut Vec<Data>>(&mut rows) };
            Box::new(move |_, row| {
                rows.extend(row);
            })
        };

        let table = if let Some((_, table)) = self.schema.get_table(&query.source.table_name) {
            table
        } else {
            return Err(format!("missing table"));
        };

        let columns = table.columns.iter().map(|c| c.name.to_owned()).collect();
        let (columns, mut appender) = build_excecutable_query_process(
            &self.schema,
            &self.storage,
            columns,
            &query.process,
            appender,
        );

        let source = self
            .storage
            .source_index(&table.name, &query.source.keys)
            .unwrap();
        let mut cursor = if let Some(from) = &query.source.from {
            self.storage.get_cursor_just(source, from)
        } else {
            self.storage.get_cursor_first(source)
        };
        let end_check_columns = query.source.to.as_ref().map(|to| {
            (
                query
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
        Ok((columns, rows))
    }

    pub fn execute_insert(&mut self, insert: &query::Insert) -> Result<(), String> {
        let values = self
            .schema
            .get_table(&insert.table_name)
            .unwrap()
            .1
            .columns
            .iter()
            .map(|column| {
                if let Some(i) = insert.column_names.iter().position(|n| &column.name == n) {
                    insert.values[i].clone()
                } else if let Some(default) = &column.default {
                    default.clone()
                } else {
                    panic!("no default")
                }
            })
            .collect();
        self.storage.add_row(&insert.table_name, values)
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
        let mut columns = columnss.last().unwrap().clone();
        match p {
            ProcessItem::Select { columns: cs } => {
                columns = cs.iter().map(|x| x.1.to_string()).collect();
            }
            ProcessItem::Filter {
                left_key,
                right_key,
            } => {}
            ProcessItem::Join {
                table_name,
                left_key,
                right_key,
            } => {
                columns = columns.clone();
                columns.extend(
                    schema
                        .get_table(table_name)
                        .unwrap()
                        .1
                        .columns
                        .iter()
                        .map(|c| format!("{}.{}", table_name, c.name)),
                );
            }
            ProcessItem::Distinct { column_name } => todo!(),
            ProcessItem::AddColumn { hoge } => todo!(),
            ProcessItem::Skip { num } => todo!(),
            ProcessItem::Limit { num } => todo!(),
        }
        columnss.push(columns);
    }

    for (p, pre_post_columns) in process.iter().zip(columnss.windows(2)).rev() {
        let pre_columns = &pre_post_columns[0];
        let post_columns = &pre_post_columns[1];

        match p {
            ProcessItem::Select { columns: cs } => {
                let map: Vec<_> = cs
                    .iter()
                    .map(|(src, _)| pre_columns.iter().position(|x| x == src).unwrap())
                    .collect();
                appender = Box::new(move |ctx, row| {
                    appender(ctx, map.iter().map(|i| row[*i].clone()).collect())
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
                        if let Some(append_row) = ctx.storage.cursor_get_row(&cursor) {
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
