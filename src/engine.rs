use crate::{
    data::Data,
    query::{ProcessItem, Query},
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

    pub fn execute_query(&self, query: &Query) -> Result<(Vec<String>, Vec<Data>), String> {
        let mut cursor = self
            .storage
            .get_const_cursor_range(0, query.source.from, query.source.to);
        let mut rows = vec![];
        let appender: RowAppender = {
            let rows = unsafe { std::mem::transmute::<*mut Vec<Data>, &mut Vec<Data>>(&mut rows) };
            Box::new(move |row| {
                rows.extend(row);
                true
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
        while {
            let row = self.storage.get_from_cursor(&cursor);
            appender(row);
            self.storage.advance_cursor(&mut cursor)
        } {}
        Ok((columns, rows))
    }
}

type RowAppender = Box<dyn FnMut(Vec<Data>) -> bool>;

fn build_excecutable_query_process(
    schema: &Schema,
    storage: &impl Storage,
    mut columns: Vec<String>,
    process: &[ProcessItem],
    mut appender: RowAppender,
) -> (Vec<String>, RowAppender) {
    for p in process {
        match p {
            ProcessItem::Select { columns: cs } => {
                let map: Vec<_> = cs
                    .iter()
                    .map(|(src, _)| columns.iter().position(|x| x == src).unwrap())
                    .collect();
                appender =
                    Box::new(move |row| appender(map.iter().map(|i| row[*i].clone()).collect()));
                columns = cs.iter().map(|x| x.1.to_string()).collect();
            }
            ProcessItem::Filter {
                left_key,
                right_key,
            } => {
                let left_i = columns.iter().position(|c| c == left_key).unwrap();
                let right_i = columns.iter().position(|c| c == right_key).unwrap();
                appender = Box::new(move |row| {
                    if row[left_i] == row[right_i] {
                        appender(row)
                    } else {
                        true
                    }
                })
            }
            ProcessItem::Join {
                table_name,
                left_key,
                right_key,
            } => todo!(),
            ProcessItem::Distinct { column_name } => todo!(),
            ProcessItem::AddColumn { hoge } => todo!(),
            ProcessItem::Skip { num } => todo!(),
            ProcessItem::Limit { num } => todo!(),
        }
    }
    (columns, appender)
}
