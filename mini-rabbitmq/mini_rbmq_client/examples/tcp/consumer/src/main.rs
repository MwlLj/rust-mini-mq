extern crate mini_rbmq_client;

use mini_rbmq_client::net::tcp;
use mini_rbmq_client::net::tcp_ack;

fn main() {
    let t = match tcp::CTcp::new("127.0.0.1:60000") {
        Ok(t) => t,
        Err(_) => return
    };
    if let Err(_) = t.connect("test-vhost") {
        // return;
    };
    if let Err(_) = t.createExchange("test_exchange", "direct") {
        // return;
    };
    if let Err(_) = t.createQueue("test_queue", "direct") {
        // return;
    };
    if let Err(_) = t.createBind("test_exchange", "test_queue", "key1") {
        // return;
    };
    if let Err(_) = t.consumer("test_queue") {
        // return;
    }
    t.consume(|ack: &tcp_ack::CAck, data: &str| {
        println!("{:?}", data);
        ack.ack();
    });
    // while let Some(data) = t.next() {
    //     println!("{:?}", &data);
    // }
    println!("success");
}
