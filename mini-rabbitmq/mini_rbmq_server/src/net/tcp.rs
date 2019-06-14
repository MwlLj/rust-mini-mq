// extern crate serde_json;
extern crate rustc_serialize;
extern crate rust_pool;
extern crate uuid;
extern crate rand;

use std::error::Error;
use std::collections::{HashMap, HashSet};
use std::io;
use std::net::TcpListener;
use std::net::TcpStream;
use std::io::BufReader;
use std::io::BufWriter;
use std::thread;
use std::time;
use std::sync::mpsc;
use std::sync::Mutex;
use std::sync::Arc;
use std::rc::Rc;
use std::cell::RefCell;
use std::io::prelude::*;

use rustc_serialize::json;

use rand::Rng;

use rust_pool::thread::simple::CThreadPool;
use super::super::storage::sqlite3;
use super::super::consts;

use uuid::Uuid;

const requestModeConnect: &str = "connect";
const requestModeCreateExchange: &str = "create-exchange";
const requestModeCreateQueue: &str = "create-queue";
const requestModeCreateBind: &str = "create-bind";
const requestModeConsumer: &str = "consumer";
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

pub struct CConsumerInfo {
    stream: TcpStream,
    connUuid: String
}

pub struct CConnect {
    consumers: HashMap<String, Vec<CConsumerInfo>>,
    queueThreadSet: HashSet<String>,
    sender: mpsc::Sender<CChannelData>,
    receiver: mpsc::Receiver<CChannelData>,
    queuePool: CThreadPool,
}

pub struct CChannelData {
    queueName: String,
    dbConn: Arc<Mutex<sqlite3::CSqlite3>>
}

impl CConnect {
    pub fn start(self, addr: &str) {
        let listener = TcpListener::bind(addr).unwrap();
        let consumers = Arc::new(Mutex::new(self.consumers));
        let threadPool = Arc::new(Mutex::new(self.queuePool));
        for stream in listener.incoming() {
            let consumers = consumers.clone();
            let threadPool = threadPool.clone();
            thread::spawn(move || {
                let connUuid = Uuid::new_v4();
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
                                Err(_) => break,
                            };
                            let request: CRequest = request;
                            if request.mode == requestModeCreateExchange {
                                // create exchange
                                if let Err(_) = dbConnect.createExchange(&request.exchangeName, &request.exchangeType) {
                                    break;
                                }
                            } else if request.mode == requestModeCreateQueue {
                                // create queue
                                if let Err(_) = dbConnect.createQueue(&request.queueName, &request.queueType) {
                                    break;
                                }
                            } else if request.mode == requestModeCreateBind {
                                // create bind
                                if let Err(_) = dbConnect.createBind(&request.exchangeName, &request.queueName, &request.routerKey) {
                                    break;
                                }
                            } else if request.mode == requestModeConsumer {
                                // consumer
                                let mut consumers = match consumers.lock() {
                                    Ok(consumers) => consumers,
                                    Err(_) => break
                                };
                                let stream = match stream.try_clone() {
                                    Ok(stream) => stream,
                                    Err(_) => break
                                };
                                let cons = CConsumerInfo{
                                    stream: stream,
                                    connUuid: connUuid.to_string(),
                                };
                                match consumers.get_mut(&request.queueName) {
                                    Some(value) => {
                                        (*value).push(cons);
                                    },
                                    None => {
                                        let mut v = Vec::new();
                                        v.push(cons);
                                        consumers.insert(request.queueName, v);
                                    },
                                };
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
                            CConnect::removeConsumer(consumers.clone(), &connUuid.to_string());
                            break;
                        };
                        if let Err(err) = writer.flush() {
                            CConnect::removeConsumer(consumers.clone(), &connUuid.to_string());
                            break;
                        };
                    }
                    {
                        // disconnect handle
                        CConnect::removeConsumer(consumers.clone(), &connUuid.to_string());
                    }
                }
            });
        }
    }
}

// private
impl CConnect {
    fn dispatch(consumers: Arc<Mutex<HashMap<String, Vec<CConsumerInfo>>>>, queueThreadSet: Arc<Mutex<HashSet<String>>>, threadMax: usize, receiver: Arc<Mutex<mpsc::Receiver<CChannelData>>>) {
        for i in 0..threadMax {
            let recv = receiver.clone();
            let queueThreadSet = queueThreadSet.clone();
            let consumers = consumers.clone();
            thread::spawn(move || {
                loop {
                    let recv = recv.clone();
                    let queueThreadSet = queueThreadSet.clone();
                    let consumers = consumers.clone();
                    let recv = match recv.lock() {
                        Ok(recv) => recv,
                        Err(_) => {
                            thread::sleep(time::Duration::from_millis(1000));
                            continue;
                        }
                    };
                    let recv = match recv.recv() {
                        Ok(recv) => recv,
                        Err(_) => {
                            thread::sleep(time::Duration::from_millis(1000));
                            continue;
                        }
                    };
                    let dbConn = match recv.dbConn.lock() {
                        Ok(dbConn) => dbConn,
                        Err(_) => {
                            continue;
                        }
                    };
                    let cons = match consumers.lock() {
                        Ok(cons) => cons,
                        Err(_) => {
                            continue;
                        }
                    };
                    let consumersList = match cons.get(&recv.queueName) {
                        Some(li) => li,
                        None => {
                            continue;
                        }
                    };
                    while let Some(data) = dbConn.getOneData(&recv.queueName, |queueType: &str, data: &str| {
                        let length = consumersList.len();
                        if queueType == consts::queue::queueTypeDirect {
                            // rand consumer
                            // random
                            let index = rand::thread_rng().gen_range(0, length);
                            let consumer = &consumersList[index];
                            let mut writer = BufWriter::new(&consumer.stream);
                            let content = vec![data.clone(), "\n"].join("");
                            writer.write_all(content.as_bytes());
                            if let Err(e) = writer.flush() {
                                CConnect::removeConsumer(Arc::clone(&consumers), &consumer.connUuid);
                            };
                        } else if queueType == consts::queue::queueTypeFanout {
                            // send to all consumer
                        }
                        return true;
                    }) {
                    }
                    // queue data is empty -> release thread
                    CConnect::removeQueueThreadSet(queueThreadSet.clone(), &recv.queueName);
                }
            });
        }
    }

    fn removeConsumer(consumers: Arc<Mutex<HashMap<String, Vec<CConsumerInfo>>>>, connUuid: &str) {
        let mut consumers = match consumers.lock() {
            Ok(cons) => cons,
            Err(_) => return
        };
        for (_, value) in consumers.iter_mut() {
            let mut index = -1 as isize;
            let mut i = 0;
            for item in &(*value) {
                if item.connUuid == connUuid.to_string() {
                    index = i;
                    break;
                }
                i += 1;
            }
            if index != -1 {
                value.remove(index as usize);
                print!("remove index: {}", &index);
            }
        }
    }

    fn notifyConsumer(queueThreadSet: Arc<Mutex<HashSet<String>>>, sender: Arc<Mutex<mpsc::Sender<CChannelData>>>, dbConn: Arc<Mutex<sqlite3::CSqlite3>>, queueName: &str) {
        let mut set = match queueThreadSet.lock() {
            Ok(set) => set,
            Err(_) => return,
        };
        let sender = match sender.lock() {
            Ok(sender) => sender,
            Err(_) => return,
        };
        match set.get(queueName) {
            Some(queue) => return,
            None => {
                if let Ok(_) = sender.send(CChannelData{
                    queueName: queueName.to_string(),
                    dbConn: dbConn,
                }) {
                    // send success -> insert to set
                    set.insert(queueName.to_string());
                }
            }
        };
    }

    fn removeQueueThreadSet(queueThreadSet: Arc<Mutex<HashSet<String>>>, queueName: &str) {
        let mut set = match queueThreadSet.lock() {
            Ok(set) => set,
            Err(_) => return,
        };
        set.remove(queueName);
    }
}

impl CConnect {
    pub fn new(queueThreadMax: usize) -> CConnect {
        let (sender, receiver): (mpsc::Sender<CChannelData>, mpsc::Receiver<CChannelData>) = mpsc::channel();
        let conn = CConnect{
            consumers: HashMap::new(),
            queueThreadSet: HashSet::new(),
            sender: sender,
            receiver: receiver,
            queuePool: CThreadPool::new(queueThreadMax),
        };
        conn
    }
}

