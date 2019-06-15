extern crate mini_rbmq_client;

use std::thread;
use std::time;

use mini_rbmq_client::net::tcp;

fn main() {
    let t = match tcp::CTcp::new("127.0.0.1:60000") {
        Ok(t) => t,
        Err(_) => return
    };
    if let Err(_) = t.connect("test-vhost") {
        return;
    };
    if let Err(_) = t.createExchange("test_exchange", "direct") {
        return;
    };
    if let Err(_) = t.createQueue("test_queue", "direct") {
        return;
    };
    if let Err(_) = t.createBind("test_exchange", "test_queue", "key1") {
        return;
    };
    if let Err(_) = t.publish("test_exchange", "key1", "hello world") {
        return;
    }
    println!("success");

    loop {
        thread::sleep(time::Duration::from_millis(1000));
    }
}
