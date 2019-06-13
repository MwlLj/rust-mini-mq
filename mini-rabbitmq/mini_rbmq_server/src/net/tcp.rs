// extern crate serde_json;
extern crate rustc_serialize;
extern crate rust_pool;

use std::error::Error;
use std::collections::HashMap;
use std::io;
use std::net::TcpListener;
use std::net::TcpStream;
use std::io::BufReader;
use std::io::BufWriter;
use std::thread;
use std::sync::mpsc;
use std::sync::Mutex;
use std::sync::Arc;
use std::rc::Rc;
use std::cell::RefCell;
use std::io::prelude::*;

use rustc_serialize::json;

use rust_pool::thread::simple::CThreadPool;
use super::super::storage::sqlite3;

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

// #[derive(Serialize, Deserialize)]
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
    data: String
}

#[derive(RustcDecodable, RustcEncodable)]
pub struct CResponse {
    error: u32,
    errorString: String
}

pub struct CSubscribeInfo {
    stream: TcpStream,
    topic: String,
    logType: String,
    serverName: String,
    serverVersion: String,
    serverNo: String
}

pub struct CConnect {
    subscribes: HashMap<String, Vec<CSubscribeInfo>>,
    queuePool: CThreadPool
}

impl CConnect {
    pub fn start(self, addr: &str) {
        let listener = TcpListener::bind(addr).unwrap();
        let subscribes = Arc::new(Mutex::new(self.subscribes));
        let threadPool = Arc::new(Mutex::new(self.queuePool));
        for stream in listener.incoming() {
            let subscribes = subscribes.clone();
            let threadPool = threadPool.clone();
            thread::spawn(move || {
                if let Ok(stream) = stream {
                    let mut reader = BufReader::new(&stream);
                    let mut writer = BufWriter::new(&stream);
                    // connect
                    let mut connect = String::new();
                    match reader.read_line(&mut connect) {
                        Ok(size) => {},
                        Err(err) => {
                            println!("{:?}", err);
                            return
                        }
                    }
                    let mut vhost = String::new();
                    match json::decode(&connect) {
                        Ok(request) => {
                            let request: CRequest = request;
                            if request.mode != requestModeConnect {
                                return;
                            }
                            vhost = request.vhost;
                        },
                        Err(err) => {
                            println!("{:?}", err);
                            return
                        }
                    }
                    let dbConnect = match sqlite3::CSqlite3::connect(&vhost) {
                        Ok(conn) => conn,
                        Err(err) => {
                            println!("{:?}", err);
                            return
                        }
                    };
                    for line in reader.lines() {
                        let line = match line {
                            Ok(line) => line,
                            Err(_) => continue,
                        };
                        let mut error: u32 = 0;
                        let mut errorString: String = String::from("success");
                        loop {
                            let request = match json::decode(&line) {
                                Ok(req) => req,
                                Err(_) => continue,
                            };
                            let request: CRequest = request;
                            // create exchange
                            if request.mode == requestModeCreateExchange {
                                if let Err(_) = dbConnect.createExchange(&request.exchangeName, &request.exchangeType) {
                                    break;
                                }
                            } else if request.mode == requestModeCreateQueue {
                                if let Err(_) = dbConnect.createQueue(&request.queueName, &request.queueType) {
                                    break;
                                }
                            } else if request.mode == requestModeCreateBind {
                                if let Err(_) = dbConnect.createBind(&request.exchangeName, &request.queueName, &request.routerKey) {
                                    break;
                                }
                            }
                            break
                        }
                        let res = CResponse{
                            error: error,
                            errorString: errorString,
                        };
                        let encode = match json::encode(&res) {
                            Ok(encode) => encode,
                            Err(_) => continue,
                        };
                        if let Err(err) = writer.write_all(encode.as_bytes()) {
                            continue
                        }
                        if let Err(err) = writer.flush() {
                            continue
                        }
                    }
                }
            });
        }
    }
}

impl CConnect {
    pub fn new(queueThreadMax: usize) -> CConnect {
        let conn = CConnect{
            subscribes: HashMap::new(),
            queuePool: CThreadPool::new(queueThreadMax),
        };
        conn
    }
}

