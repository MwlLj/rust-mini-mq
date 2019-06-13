extern crate mini_rbmq_server;

use mini_rbmq_server::storage::{IStorage, sqlite3};
use mini_rbmq_server::consts::exchange;
use mini_rbmq_server::consts::queue;
use mini_rbmq_server::net::tcp;

fn dbTest() {
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
        if let Err(_) = s.createQueue("test_queue1", queue::queueTypeDirect) {
            return;
        }
        println!("create queue1 success");

        if let Err(_) = s.createQueue("test_queue2", queue::queueTypeDirect) {
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
    while let Some(data) = s.getOneData("test_queue2", |queueType: &str, data: &str| -> bool {
        return true;
    }) {
        println!("{:?}", &data);
    }
    let mut buffer = vec![0 as u8; 100];
    buffer[0] = 65;
    buffer[1] = 66;
    buffer[2] = 67;
    buffer[3] = 68;
    println!("{:?}", &String::from_utf8(buffer[..4].to_vec()));
    println!("{:?}", &buffer[5..]);
}

fn tcpTest() {
    let conn = tcp::CConnect::new(10);
    conn.start("0.0.0.0:60000");
}

fn main() {
    // dbTest();
    tcpTest();
}
