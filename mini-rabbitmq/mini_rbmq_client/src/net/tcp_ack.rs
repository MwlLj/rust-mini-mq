extern crate rust_parse;
extern crate rustc_serialize;

use std::sync::{
    Arc, Mutex
};
use std::io::prelude::*;
use std::io::BufWriter;
use std::net::TcpStream;

use rustc_serialize::json;

use super::super::consts::define;

const requestModeAck: &str = "ack";

#[derive(RustcDecodable, RustcEncodable)]
pub struct CRequest {
    mode: String,
    identify: String,
    vhost: String,
    exchangeName: String,
    exchangeType: String,
    queueName: String,
    queueType: String,
    routerKey: String,
    data: String,
    ackResult: String
}

pub struct CAck(TcpStream);

impl CAck {
    pub fn ack(&self) -> Result<(), &str> {
        let mut writer = BufWriter::new(&self.0);
        let request = CRequest {
            mode: requestModeAck.to_string(),
            identify: "".to_string(),
            vhost: "".to_string(),
            exchangeName: "".to_string(),
            exchangeType: "".to_string(),
            queueName: "".to_string(),
            queueType: "".to_string(),
            routerKey: "".to_string(),
            data: "".to_string(),
            ackResult: define::ackTrue.to_string()
        };
        let encoded = match json::encode(&request) {
            Ok(encoded) => encoded,
            Err(_) => return Err("encode error")
        };
        if let Err(_) = writer.write_all(CAck::joinLineFeed(&encoded).as_bytes()) {
            return Err("write all error");
        }
        if let Err(_) = writer.flush() {
            return Err("flush error");
        }
        Ok(())
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

