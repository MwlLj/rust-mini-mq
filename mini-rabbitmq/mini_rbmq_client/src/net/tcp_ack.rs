use std::sync::{
    Arc, Mutex
};
use std::io::prelude::*;
use std::io::BufWriter;
use std::net::TcpStream;

pub struct CAck(TcpStream);

impl CAck {
    pub fn ack(&self) {
        let mut writer = BufWriter::new(&self.0);
        println!("1");
        writer.write_all(CAck::joinLineFeed("true").as_bytes());
        writer.flush();
        println!("2");
    }

    pub fn new(stream: TcpStream) -> CAck {
        CAck(stream)
    }
}

impl CAck {
    fn joinLineFeed(content: &str) -> String {
        return vec![content, "\n"].join("");
    }
}

