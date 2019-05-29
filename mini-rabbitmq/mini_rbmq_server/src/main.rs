extern crate mini_rbmq_server;

use mini_rbmq_server::storage::{IStorage, sqlite3};

fn main() {
    let s = &sqlite3::CSqlite3::connect("test");
    if let Ok(ref s) = s {
        s.createExchange("test_change", "fanout");
    }
}
