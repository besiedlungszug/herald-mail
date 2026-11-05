use std::time::Duration;
use std::env;
use std::path::Path;
use uuid::Uuid;
use tokio::{
    task::JoinSet,
    sync::mpsc,
};

use mysql_binlog_connector_rust::{
    binlog_client::{BinlogClient, StartPosition},
};

use herald::sqlx::{Connection, MySqlConnection};

mod binlog;
mod template;

#[tokio::main]
async fn main() -> () {
    const ERROR_SETUP: &str = "Error while reading environment configuration";
    let url = env::var("DATABASE_URL").expect(ERROR_SETUP);

    let (upd_tx, upd_rx) = mpsc::channel::<(Uuid, u64)>(8);
    let (rnd_tx, mut rnd_rx) = mpsc::channel::<String>(16);

    let mut upd_client = BinlogClient::new(&url, 9, StartPosition::Gtid("".to_owned()))
        .with_master_heartbeat(Duration::from_secs(5))
        .with_read_timeout(Duration::from_secs(60))
        .with_keepalive(Duration::from_secs(60), Duration::from_secs(10));
    let upd_stream = upd_client.connect();
    let rnd_conn = MySqlConnection::connect(&url);
    let mut tasks = JoinSet::<()>::new();
    tasks.spawn(crate::binlog::listener(upd_stream.await.unwrap(), upd_tx));
    tasks.spawn(crate::template::renderer(rnd_conn.await.unwrap(), upd_rx, rnd_tx));


    let mut target = Path::new("mail").to_path_buf();
    let mut buf = [b'.'; 40];
    buf[37..].copy_from_slice(b"eml");
    loop {
        let message = rnd_rx.recv().await.expect("Error while reading 'rnd': closed handle");
        Uuid::now_v7().as_hyphenated().encode_lower(&mut buf);
        let filename = str::from_utf8(&buf).unwrap();
        target.push(&filename);
        if let Err(_) = std::fs::write(&target, message) {
            //say something about failed writes
        }
        target.pop();
    }
}
