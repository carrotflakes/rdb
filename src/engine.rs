use crate::{
    data::Data,
    query::{ProcessItem, Query},
    schema::Schema,
    storage::StorageOld,
};

pub struct Engine<S: StorageOld> {
    schema: Schema,
    storage: S,
}

impl<S: StorageOld> Engine<S> {
    pub fn new(schema: Schema, storage: S) -> Self {
        Self { schema, storage }
    }

    pub fn execute_query(&self, query: &Query) -> Result<(Vec<String>, Vec<Data>), String> {
        let mut rows = vec![];
        let appender: RowAppender<_> = {
            let rows = unsafe { std::mem::transmute::<*mut Vec<Data>, &mut Vec<Data>>(&mut rows) };
            Box::new(move |_, row| {
                rows.extend(row);
            })
        };
        let columns = self
            .schema
            .get_table(&query.source.table_name)
            .unwrap()
            .1
            .columns
            .iter()
            .map(|c| c.name.to_owned())
            .collect();
        let (columns, mut appender) = build_excecutable_query_process(
            &self.schema,
            &self.storage,
            columns,
            &query.process,
            appender,
        );

        let source = self.storage.source_index(&query.source.table_name).unwrap();
        let mut cursor =
            self.storage
                .get_const_cursor_range(source, query.source.from, query.source.to);
        let mut ctx = QueryContext {
            storage: &self.storage,
            ended: false,
        };
        while !ctx.ended && !self.storage.cursor_is_end(&cursor) && {
            let row = self.storage.get_from_cursor(&cursor);
            appender(&mut ctx, row);
            self.storage.advance_cursor(&mut cursor)
        } {}

        Ok((columns, rows))
    }
}

pub struct QueryContext<'a, S: StorageOld> {
    storage: &'a S,
    ended: bool,
}

type RowAppender<S> = Box<dyn for<'a> FnMut(&mut QueryContext<'a, S>, Vec<Data>)>;

fn build_excecutable_query_process<S: StorageOld>(
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
                let source = storage.source_index(table_name).unwrap();
                let left_i = post_columns.iter().position(|c| c == left_key).unwrap();
                // let right_i = columns.iter().position(|c| c == right_key).unwrap(); // TODO!!!!
                appender = Box::new(move |ctx, row| {
                    let mut cursor = ctx
                        .storage
                        .get_const_cursor_just(source, row[left_i].clone());
                    if ctx.storage.cursor_is_end(&cursor) {
                        return;
                    }
                    while {
                        let append_row = ctx.storage.get_from_cursor(&cursor);
                        let mut row_ = row.clone();
                        row_.extend(append_row);
                        appender(ctx, row_);
                        ctx.storage.advance_cursor(&mut cursor)
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
