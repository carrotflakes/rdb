use std::sync::Arc;

use tokio::sync::{watch, Mutex};

pub struct LockInner {
    tables: Vec<Table>,
}

#[derive(Default)]
pub struct Table {
    read_rows: Vec<(usize, usize, watch::Receiver<bool>, usize)>,
    write_rows: Vec<(usize, usize, watch::Receiver<bool>, usize)>,
}

#[derive(Clone)]
pub struct Lock(Arc<Mutex<LockInner>>);

pub struct User {
    id: usize, 
    senders: Vec<watch::Sender<bool>>
}

impl User {
    fn new(id: usize) -> Self {
        User {id, senders: Vec::new()}
    }
}

impl Lock {
    pub fn new() -> Self {
        let lock = Lock(Arc::new(Mutex::new(LockInner {
            tables: vec![Table::default()],
        })));

        // TODO: garbage collect

        lock
    }

    pub async fn read_row_with_lock(
        &self,
        user: &mut User,
        table_index: usize,
        start: usize,
        end: usize,
        timeout: usize,
    ) -> Result<(), ()> {
        let (tx, rx) = watch::channel(false);
        user.senders.push(tx);

        // 関係するロックを取得
        let locks = {
            let mut lock = self.0.lock().await;
            let locks = lock.tables[table_index]
                .read_rows
                .iter()
                .filter(|l| l.3 != user.id && (l.0 <= start || l.0 <= end) && (start <= l.1 || end <= l.1))
                .map(|l| l.2.clone())
                .collect::<Vec<_>>();

            // ロックキューにpush
            lock.tables[table_index].read_rows.push((start, end, rx, user.id)); // TODO: ここでpushしちゃだめ！？
            locks
        };

        // 関係するロックが解除されるのを待つ
        // TODO: timeout
        for mut lock in locks {
            while !*lock.borrow() {
                lock.changed().await.unwrap(); // TODO: droped?
            }
        }
        Ok(())
    }

    pub async fn write_row(
        &self,
        user: &mut User,
        table_index: usize,
        start: usize,
        end: usize,
        timeout: usize,
    ) {
        todo!()
    }
}

impl Drop for User {
    fn drop(&mut self) {
        for tx in &self.senders {
            tx.send(true).unwrap();
        }
    }
}

#[tokio::test]
async fn test() {
    let lock = Lock::new();
    tokio::spawn({
        let lock = lock.clone();
        async move {
            let mut user = User::new(0);
            lock.read_row_with_lock(&mut user, 0, 1, 2, 0).await;
            dbg!("ok1");
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            dbg!("ok3");
            lock.read_row_with_lock(&mut user, 0, 1, 3, 0).await;
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            dbg!("ok4");
        }
    });
    let mut user = User::new(1);
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    lock.read_row_with_lock(&mut user, 0, 1, 2, 0).await;
    dbg!("ok2");
}
