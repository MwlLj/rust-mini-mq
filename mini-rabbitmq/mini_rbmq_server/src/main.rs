extern crate mini_rbmq_server;

use mini_rbmq_server::storage::sqlite3;

fn main() {
    let s = sqlite3::CSqlite3::new();
}
