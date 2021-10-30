use tokio::sync::RwLock;

use crate::{data::Data, file::File, lock::Lock, query, schema::Schema};

pub struct LockedStorage {
    storage: RwLock<File>,
    lock: Lock,
}

impl LockedStorage {
    pub fn new(storage: File) -> Self {
        Self {
            storage: RwLock::new(storage),
            lock: Lock::new(),
        }
    }

    pub async fn select(&self, select: &query::Select) -> Result<(Vec<String>, Vec<Data>), String> {
        // TODO: lock

        // let mut storage = self.storage.read().await;

        // Ok((columns, rows))

        todo!()
    }
}
