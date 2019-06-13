extern crate rust_parse;
extern crate rustc_serialize;

use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;
use std::net::TcpStream;
use std::thread;

use rustc_serialize::json;
use rust_parse::cmd::CCmd;

const requestModeConnect: &str = "connect";
const requestModeCreateExchange: &str = "create-exchange";
const requestModeCreateQueue: &str = "create-queue";
const requestModeCreateBind: &str = "create-bind";
const requestIdentifyPublish: &str = "publish";
const requestIdentifySubscribe: &str = "subscribe";
const storageModeNone: &str = "none";
const storageModeFile: &str = "file";
const logTypeMessage: &str = "message";
const logTypeError: &str = "error";

const argServer: &str = "-server";
const argServerName: &str = "-server-name";
const argServerVersion: &str = "-server-version";
const argServerNo: &str = "-server-no";
const argData: &str = "-data";
const argStorageMode: &str = "-storage-mode";
const argLogType: &str = "-log-type";
const argTopic: &str = "-topic";

#[derive(RustcDecodable, RustcEncodable)]
struct CRequest {
    mode: String,
    identify: String,
    vhost: String,
    exchangeName: String,
    exchangeType: String,
    queueName: String,
    queueType: String,
    routerKey: String,
    data: String
}

pub struct CTcp {
    stream: TcpStream
}

impl CTcp {
    pub fn connect(&self, vhost: &str) -> Result<(), &str> {
        let mut writer = BufWriter::new(&self.stream);
        let connRequest = CRequest {
            mode: requestModeConnect.to_string(),
            identify: "".to_string(),
            vhost: vhost.to_string(),
            exchangeName: "".to_string(),
            exchangeType: "".to_string(),
            queueName: "".to_string(),
            queueType: "".to_string(),
            routerKey: "".to_string(),
            data: "".to_string()
        };
        let encoded = match json::encode(&connRequest) {
            Ok(encoded) => encoded,
            Err(_) => return Err("json encode error")
        };
        let content = vec![encoded, "\n".to_string()].join("");
        if let Err(err) = writer.write_all(content.as_bytes()) {
            return Err("write error");
        }
        if let Err(err) = writer.flush() {
            return Err("flush error");
        }
        Ok(())
    }

    pub fn createExchange(&self, exchangeName: &str, exchangeType: &str) -> Result<(), &str> {
        let mut writer = BufWriter::new(&self.stream);
        let connRequest = CRequest {
            mode: requestModeCreateExchange.to_string(),
            identify: "".to_string(),
            vhost: "".to_string(),
            exchangeName: exchangeName.to_string(),
            exchangeType: exchangeType.to_string(),
            queueName: "".to_string(),
            queueType: "".to_string(),
            routerKey: "".to_string(),
            data: "".to_string()
        };
        let encoded = match json::encode(&connRequest) {
            Ok(encoded) => encoded,
            Err(_) => return Err("json encode error")
        };
        let content = vec![encoded, "\n".to_string()].join("");
        if let Err(err) = writer.write_all(content.as_bytes()) {
            return Err("write error");
        }
        if let Err(err) = writer.flush() {
            return Err("flush error");
        }
        Ok(())
    }

    pub fn createQueue(&self, queueName: &str, queueType: &str) -> Result<(), &str> {
        let mut writer = BufWriter::new(&self.stream);
        let connRequest = CRequest {
            mode: requestModeCreateQueue.to_string(),
            identify: "".to_string(),
            vhost: "".to_string(),
            exchangeName: "".to_string(),
            exchangeType: "".to_string(),
            queueName: queueName.to_string(),
            queueType: queueType.to_string(),
            routerKey: "".to_string(),
            data: "".to_string()
        };
        let encoded = match json::encode(&connRequest) {
            Ok(encoded) => encoded,
            Err(_) => return Err("json encode error")
        };
        let content = vec![encoded, "\n".to_string()].join("");
        if let Err(err) = writer.write_all(content.as_bytes()) {
            return Err("write error");
        }
        if let Err(err) = writer.flush() {
            return Err("flush error");
        }
        Ok(())
    }

    pub fn createBind(&self, exchangeName: &str, queueName: &str, routerKey: &str) -> Result<(), &str> {
        let mut writer = BufWriter::new(&self.stream);
        let connRequest = CRequest {
            mode: requestModeCreateBind.to_string(),
            identify: "".to_string(),
            vhost: "".to_string(),
            exchangeName: exchangeName.to_string(),
            exchangeType: "".to_string(),
            queueName: queueName.to_string(),
            queueType: "".to_string(),
            routerKey: routerKey.to_string(),
            data: "".to_string()
        };
        let encoded = match json::encode(&connRequest) {
            Ok(encoded) => encoded,
            Err(_) => return Err("json encode error")
        };
        let content = vec![encoded, "\n".to_string()].join("");
        if let Err(err) = writer.write_all(content.as_bytes()) {
            return Err("write error");
        }
        if let Err(err) = writer.flush() {
            return Err("flush error");
        }
        Ok(())
    }
}

impl CTcp {
    pub fn new(addr: &str) -> Result<CTcp, std::io::Error> {
        let conn = match TcpStream::connect(addr) {
            Ok(conn) => conn,
            Err(err) => return Err(err)
        };
        Ok(CTcp{
            stream: conn
        })
    }
}

/*
fn main() {
    let mut cmdHandler = CCmd::new();
    let server = cmdHandler.register(argServer, "127.0.0.1:50005");
    let serverName = cmdHandler.register(argServerName, "tests");
    let serverVersion = cmdHandler.register(argServerVersion, "1.0");
    let serverNo = cmdHandler.register(argServerNo, "1");
    let data = cmdHandler.register(argData, "hello");
    let storageMode = cmdHandler.register(argStorageMode, storageModeFile);
    let logType = cmdHandler.register(argLogType, logTypeMessage);
    let topic = cmdHandler.register(argTopic, "");
    cmdHandler.parse();

    let server = server.borrow();
    let serverName = serverName.borrow();
    let serverVersion = serverVersion.borrow();
    let serverNo = serverNo.borrow();
    let data = data.borrow();
    let storageMode = storageMode.borrow();
    let logType = logType.borrow();
    let topic = topic.borrow();

    let stream = TcpStream::connect(&(*server)).unwrap();
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    {
        let connRequest = CRequest {
            mode: requestModeConnect.to_string(),
            identify: requestIdentifyPublish.to_string(),
            serverName: serverName.to_string(),
            serverVersion: serverVersion.to_string(),
            serverNo: serverNo.to_string(),
            topic: "".to_string(),
            data: "".to_string(),
            storageMode: "".to_string(),
            logType: "".to_string()
        };
        let encoded = json::encode(&connRequest).unwrap();
        let content = vec![encoded, "\n".to_string()].join("");
        writer.write_all(content.as_bytes()).unwrap();
        writer.flush().unwrap();
    }

    loop {
        let pubRequest = CRequest {
            mode: requestModeSending.to_string(),
            identify: requestIdentifyPublish.to_string(),
            serverName: serverName.to_string(),
            serverVersion: serverVersion.to_string(),
            serverNo: serverNo.to_string(),
            topic: topic.to_string(),
            data: data.to_string(),
            storageMode: storageMode.to_string(),
            logType: logType.to_string()
        };
        let encoded = json::encode(&pubRequest).unwrap();
        let content = vec![encoded, "\n".to_string()].join("");
        writer.write_all(content.as_bytes()).unwrap();
        writer.flush().unwrap();

        thread::sleep_ms(1000);
    }
}
*/