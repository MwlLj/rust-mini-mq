extern crate rust_parse;
extern crate rustc_serialize;
extern crate uuid;

use std::sync::{
    Arc, Mutex
};
use std::io::prelude::*;
use std::io::BufWriter;
use std::net::TcpStream;

use rustc_serialize::json;

use uuid::Uuid;

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
    ackResult: String,
    messageNo: String
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
            ackResult: define::ackTrue.to_string(),
            messageNo: self.genUuid()
        };
        if let Err(err) = self.sendRequest(request) {
            return Err("send eror");
        };
        /*
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
        */
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

    fn genUuid(&self) -> String {
        Uuid::new_v4().to_string()
    }
    
    fn append32Number(&self, value: u32, buf: &mut Vec<u8>) {
        for i in 0..32 {
            let b = (value >> i) & 1;
            buf.push(b as u8);
        }
    }

    fn sendRequest(&self, request: CRequest) -> Result<(), &str> {
        let mut writer = BufWriter::new(&self.0);
        let mut buf = Vec::new();
        self.append32Number(request.mode.len() as u32, &mut buf);
        buf.append(&mut request.mode.as_bytes().to_vec());
        self.append32Number(request.identify.len() as u32, &mut buf);
        buf.append(&mut request.identify.as_bytes().to_vec());
        self.append32Number(request.vhost.len() as u32, &mut buf);
        buf.append(&mut request.vhost.as_bytes().to_vec());
        self.append32Number(request.exchangeName.len() as u32, &mut buf);
        buf.append(&mut request.exchangeName.as_bytes().to_vec());
        self.append32Number(request.exchangeType.len() as u32, &mut buf);
        buf.append(&mut request.exchangeType.as_bytes().to_vec());
        self.append32Number(request.queueName.len() as u32, &mut buf);
        buf.append(&mut request.queueName.as_bytes().to_vec());
        self.append32Number(request.queueType.len() as u32, &mut buf);
        buf.append(&mut request.queueType.as_bytes().to_vec());
        self.append32Number(request.routerKey.len() as u32, &mut buf);
        buf.append(&mut request.routerKey.as_bytes().to_vec());
        self.append32Number(request.data.len() as u32, &mut buf);
        buf.append(&mut request.data.as_bytes().to_vec());
        self.append32Number(request.ackResult.len() as u32, &mut buf);
        buf.append(&mut request.ackResult.as_bytes().to_vec());
        self.append32Number(request.messageNo.len() as u32, &mut buf);
        buf.append(&mut request.messageNo.as_bytes().to_vec());
        if let Err(err) = writer.write_all(&buf) {
            return Err("write error");
        };
        if let Err(err) = writer.flush() {
            return Err("flush error");
        };
        Ok(())
    }
}

