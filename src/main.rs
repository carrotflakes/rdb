use tokio::{sync::Notify, time::{Instant, sleep_until, Duration}};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let notify = Arc::new(Notify::new());
    let notify2 = notify.clone();
    let notify3 = notify.clone();

    let handle = tokio::spawn(async move {
        sleep_until(Instant::now() + Duration::from_millis(100)).await;
        notify.notified().await;
        println!("received notifications");
    });
    
    let handle = tokio::spawn(async move {
        sleep_until(Instant::now() + Duration::from_millis(200)).await;
        notify2.notified().await;
        println!("received notifications");
    });
    
    // let handle = tokio::spawn(async move {
        println!("sending notifications");
        sleep_until(Instant::now() + Duration::from_millis(300)).await;
        notify3.notify_waiters();
    // });
        sleep_until(Instant::now() + Duration::from_millis(500)).await;
}
