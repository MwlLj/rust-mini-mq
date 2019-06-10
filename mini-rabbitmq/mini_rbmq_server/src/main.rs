extern crate mini_rbmq_server;

use mini_rbmq_server::storage::{IStorage, sqlite3};
use mini_rbmq_server::consts::exchange;

fn main() {
    let s = &sqlite3::CSqlite3::connect("test");
    let s = match s {
        Ok(s) => s,
        Err(_) => return,
    };
    println!("connect success");
    if let Err(_) = s.createExchange("test_change", exchange::exchangeTypeDirect) {
        return;
    }
    println!("create exchange success");
    {
        if let Err(_) = s.createQueue("test_queue1") {
            return;
        }
        println!("create queue1 success");

        if let Err(_) = s.createQueue("test_queue2") {
            return;
        }
        println!("create queue2 success");
    }
    {
        if let Err(_) = s.createBind("test_change", "test_queue1", "test_key") {
            return;
        }
        println!("create bind 1 success");

        if let Err(_) = s.createBind("test_change", "test_queue2", "test_key") {
            return;
        }
        println!("create bind 2 success");
    }
    if let Err(_) = s.addData("test_change", "test_key", "test data") {
        return;
    }
    println!("add data success");
}
