use std::sync::Arc;

use crate::{data::Data, locked_storage::LockedStorage, query};

pub struct Transaction {
    locked_storage: Arc<LockedStorage>,
}

impl Transaction {
    pub fn new(locked_storage: Arc<LockedStorage>) -> Self {
        Self { locked_storage }
    }

    pub async fn commit(&mut self) {
        todo!()
    }
}

impl Transaction {
    // rows: select, insert, update, delete
    pub async fn select(
        &mut self,
        select: &query::Select,
    ) -> Result<(Vec<String>, Vec<Data>), String> {
        self.locked_storage.select(select).await
    }
    // table: create table, drop table
    // columns: add, delete, change
    // index: create, drop
    // constraint: add, delete
}

#[tokio::test]
async fn test() {
    let file = crate::file::File::open("transaction.rdb");
    let locked_storage = Arc::new(LockedStorage::new(file));
    let mut t = Transaction::new(locked_storage);
    dbg!(
        &t.select(&query::Select {
            sub_queries: vec![],
            streams: vec![query::Stream {
                source: query::SelectSource::Table(query::SelectSourceTable {
                    table_name: "user".to_owned(),
                    keys: vec![],
                    from: None,
                    to: None,
                }),
                process: vec![],
            }],
            post_process: vec![],
        })
        .await,
    );
    // t.insert()
}
