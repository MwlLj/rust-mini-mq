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
const requestModePublish: &str = "publish";
const requestModeConsumer: &str = "consumer";
const requestModeAck: &str = "ack";

const responseModeResult: &str = "result";
const responseModeData: &str = "data";

const ackResultSuccess: u32 = 0;
const ackResultFailed: u32 = 1;
const ackResultTrue: u32 = 2;
const ackResultFalse: u32 = 3;

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
    data: String,
    ackResult: String
}

#[derive(RustcDecodable, RustcEncodable)]
pub struct CResponse {
    mode: String,
    queueName: String,
    data: String,
    error: u32,
    errorString: String
}

struct CAckInfo {
    ackResult: String
}

pub struct CConsumerInfo {
    stream: TcpStream,
    connUuid: String,
    ackReceiver: mpsc::Receiver<CAckInfo>
}

struct CAckSender {
    ackSender: mpsc::Sender<CAckInfo>
}

pub struct CTcp {
    connects: HashMap<String, CConnect>,
    threadMax: usize
}

struct CConnect {
    consumers: HashMap<String, Vec<CConsumerInfo>>,
    acks: HashMap<String, CAckSender>,
    queueThreadSet: HashSet<String>,
    sender: mpsc::Sender<CChannelData>,
    receiver: mpsc::Receiver<CChannelData>
}

pub struct CChannelData {
    queueName: String,
    dbConn: Arc<Mutex<sqlite3::CSqlite3>>
}

impl CTcp {
    pub fn start(self, addr: &str) {
        let listener = TcpListener::bind(addr).unwrap();
        let connects = Arc::new(Mutex::new(self.connects));
        let threadMax = Arc::new(Mutex::new(self.threadMax));
        // let consumers = Arc::new(Mutex::new(self.consumers));
        // let acks = Arc::new(Mutex::new(self.acks));
        // let threadPool = Arc::new(Mutex::new(self.queuePool));
        // let queueThreadSet = Arc::new(Mutex::new(self.queueThreadSet));
        // let sender = Arc::new(Mutex::new(self.sender));
        // let receiver = Arc::new(Mutex::new(self.receiver));
        for stream in listener.incoming() {
            let connects = connects.clone();
            let threadMax = threadMax.clone();
            // let consumers = consumers.clone();
            // let acks = acks.clone();
            // let threadPool = threadPool.clone();
            // let queueThreadSet = queueThreadSet.clone();
            // let sender = sender.clone();
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
                    let threadMax = match threadMax.lock() {
                        Ok(threadMax) => threadMax,
                        Err(err) => {
                            println!("{:?}", err);
                            return;
                        },
                    };
                    let dbConnect = Arc::new(Mutex::new(match sqlite3::CSqlite3::connect(&vhost) {
                        Ok(conn) => conn,
                        Err(err) => {
                            println!("{:?}", err);
                            return
                        }
                    }));
                    let connects = match connects.lock() {
                        Ok(connects) => connects,
                        Err(_) => return,
                    };
                    let connect = match connects.get(&vhost) {
                        Some(connect) => connect,
                        None => {
                            let (sender, receiver): (mpsc::Sender<CChannelData>, mpsc::Receiver<CChannelData>) = mpsc::channel();
                            CConnect{
                                consumers: HashMap::new(),
                                acks: HashMap::new(),
                                queueThreadSet: HashSet::new(),
                                sender: sender,
                                receiver: receiver
                            }
                        },
                    };
                    let consumers = Arc::new(Mutex::new(connect.consumers));
                    let acks = Arc::new(Mutex::new(connect.acks));
                    let queueThreadSet = Arc::new(Mutex::new(connect.queueThreadSet));
                    let sender = Arc::new(Mutex::new(connect.sender));
                    let receiver = Arc::new(Mutex::new(connect.receiver));
                    CTcp::dispatch(
                        consumers.clone(),
                        acks.clone(),
                        queueThreadSet.clone(),
                        *threadMax,
                        receiver.clone());
                    for line in reader.lines() {
                        let line = match line {
                            Ok(line) => line,
                            Err(_) => {
                                println!("disconnect");
                                break;
                            }
                        };
                        let mut error: u32 = consts::result::resultOkError;
                        let mut errorString: String = consts::result::resultOkErrorString.to_string();
                        loop {
                            let request = match json::decode(&line) {
                                Ok(req) => req,
                                Err(_) => break,
                            };
                            let request: CRequest = request;
                            if request.mode == requestModeCreateExchange {
                                // create exchange
                                let dbConn = match dbConnect.lock() {
                                    Ok(dbConn) => dbConn,
                                    Err(_) => break
                                };
                                if let Err(_) = dbConn.createExchange(&request.exchangeName, &request.exchangeType) {
                                    break;
                                }
                            } else if request.mode == requestModeCreateQueue {
                                // create queue
                                let dbConn = match dbConnect.lock() {
                                    Ok(dbConn) => dbConn,
                                    Err(_) => break
                                };
                                if let Err(_) = dbConn.createQueue(&request.queueName, &request.queueType) {
                                    break;
                                }
                            } else if request.mode == requestModeCreateBind {
                                // create bind
                                let dbConn = match dbConnect.lock() {
                                    Ok(dbConn) => dbConn,
                                    Err(_) => break
                                };
                                if let Err(_) = dbConn.createBind(&request.exchangeName, &request.queueName, &request.routerKey) {
                                    break;
                                }
                            } else if request.mode == requestModePublish {
                                // create bind
                                let dbConn = match dbConnect.lock() {
                                    Ok(dbConn) => dbConn,
                                    Err(_) => break
                                };
                                if let Ok(queues) = dbConn.addData(&request.exchangeName, &request.routerKey, &request.data) {
                                    for queue in queues {
                                        CTcp::notifyConsumer(consumers.clone(), queueThreadSet.clone(), sender.clone(), dbConnect.clone(), &queue);
                                    }
                                } else {
                                    break;
                                }
                            } else if request.mode == requestModeConsumer {
                                // consumer
                                {
                                    let mut consumes = match consumers.lock() {
                                        Ok(consumes) => consumes,
                                        Err(_) => break
                                    };
                                    let mut acks = match acks.lock() {
                                        Ok(acks) => acks,
                                        Err(_) => break,
                                    };
                                    let stream = match stream.try_clone() {
                                        Ok(stream) => stream,
                                        Err(_) => break
                                    };
                                    let (s, r) = mpsc::channel();
                                    let cons = CConsumerInfo{
                                        stream: stream,
                                        connUuid: connUuid.to_string(),
                                        ackReceiver: r
                                    };
                                    match consumes.get_mut(&request.queueName) {
                                        Some(value) => {
                                            (*value).push(cons);
                                        },
                                        None => {
                                            let mut v = Vec::new();
                                            v.push(cons);
                                            consumes.insert(request.queueName.to_string(), v);
                                        },
                                    };
                                    match acks.get_mut(&connUuid.to_string()) {
                                        Some(value) => {
                                            (*value).ackSender = s;
                                        },
                                        None => {
                                            acks.insert(connUuid.to_string(), CAckSender{
                                                ackSender: s,
                                            });
                                        },
                                    };
                                }
                                CTcp::notifyConsumer(consumers.clone(), queueThreadSet.clone(), sender.clone(), dbConnect.clone(), &request.queueName);
                            } else if request.mode == requestModeAck {
                                // ack
                                let mut acks = match acks.try_lock() {
                                    Ok(acks) => acks,
                                    Err(_) => {
                                        println!("try lock error");
                                        break;
                                    }
                                };
                                if let Some(send) = acks.get(&connUuid.to_string()) {
                                    if let Err(err) = send.ackSender.send(CAckInfo{
                                        ackResult: request.ackResult.to_string(),
                                    }) {
                                        println!("ackSender send error: {}", err);
                                    }
                                }
                            }
                            break
                        }
                        let res = CResponse{
                            mode: responseModeResult.to_string(),
                            queueName: "".to_string(),
                            data: "".to_string(),
                            error: error,
                            errorString: errorString,
                        };
                        let encode = match json::encode(&res) {
                            Ok(encode) => encode,
                            Err(_) => continue,
                        };
                        if let Err(err) = writer.write_all(CTcp::joinLineFeed(&encode).as_bytes()) {
                            println!("send response write all error");
                            // CTcp::removeConsumer(consumers.clone(), acks.clone(), &connUuid.to_string());
                            break;
                        };
                        if let Err(err) = writer.flush() {
                            println!("send response flush error, {}", err);
                            // CTcp::removeConsumer(consumers.clone(), acks.clone(), &connUuid.to_string());
                            break;
                        };
                    }
                    {
                        // disconnect handle
                        println!("remove consumer");
                        CTcp::removeConsumer(consumers.clone(), acks.clone(), &connUuid.to_string());
                    }
                }
            });
        }
    }
}

// private
impl CTcp {
    fn dispatch(consumers: Arc<Mutex<HashMap<String, Vec<CConsumerInfo>>>>, acks: Arc<Mutex<HashMap<String, CAckSender>>>, queueThreadSet: Arc<Mutex<HashSet<String>>>, threadMax: usize, receiver: Arc<Mutex<mpsc::Receiver<CChannelData>>>) {
        for i in 0..threadMax {
            let recv = receiver.clone();
            let queueThreadSet = queueThreadSet.clone();
            let consumers = consumers.clone();
            let acks = acks.clone();
            thread::spawn(move || {
                loop {
                    let recv = recv.clone();
                    let queueThreadSet = queueThreadSet.clone();
                    let consumers = consumers.clone();
                    let acks = acks.clone();
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
                            println!("get dbconn error");
                            continue;
                        }
                    };
                    let cons = match consumers.lock() {
                        Ok(cons) => cons,
                        Err(_) => {
                            println!("get consumers error");
                            return false;
                        }
                    };
                    let consumersList = match cons.get(&recv.queueName) {
                        Some(li) => {
                            li
                        },
                        None => {
                            println!("consumer is not found");
                            return false;
                        }
                    };
                    let length = consumersList.len();
                    if length == 0 {
                        continue;
                    }
                    while let Some(data) = dbConn.getOneData(&recv.queueName, |queueType: &str, data: &str| {
                        let length = consumersList.len();
                        if length == 0 {
                            return false;
                        }
                        if queueType == consts::queue::queueTypeDirect {
                            // rand consumer
                            // random
                            if length > 0 {
                                let index = rand::thread_rng().gen_range(0, length);
                                let consumer = &consumersList[index];
                                let r = CTcp::sendToConsumer(consumers.clone(), Arc::clone(&acks), &consumer, &recv.queueName, data);
                                if r == ackResultFalse || r == ackResultFailed {
                                    println!("error 1");
                                    return false;
                                }
                            }
                        } else if queueType == consts::queue::queueTypeFanout {
                            // send to all consumer
                            for consumer in consumersList {
                                let r = CTcp::sendToConsumer(consumers.clone(), Arc::clone(&acks), &consumer, &recv.queueName, data);
                                if r == ackResultFalse || r == ackResultFailed {
                                    println!("error 2");
                                    return false;
                                }
                            }
                        }
                        return true;
                    }) {
                    }
                    println!("exit consumer");
                    // queue data is empty -> release thread
                    CTcp::removeQueueThreadSet(queueThreadSet.clone(), &recv.queueName);
                }
            });
        }
    }

    fn sendToConsumer(consumers: Arc<Mutex<HashMap<String, Vec<CConsumerInfo>>>>, acks: Arc<Mutex<HashMap<String, CAckSender>>>, consumer: &CConsumerInfo, queueName: &str, data: &str) -> u32 {
        // let consumer = match consumer.lock() {
        //     Ok(consumer) => consumer,
        //     Err(_) => return,
        // };
        let mut writer = BufWriter::new(&consumer.stream);
        let encode = match json::encode(&CResponse{
            mode: responseModeData.to_string(),
            queueName: queueName.to_string(),
            data: data.to_string(),
            error: consts::result::resultOkError,
            errorString: consts::result::resultOkErrorString.to_string()
        }) {
            Ok(encode) => encode,
            Err(_) => {
                println!("encode data error, data: {}", data);
                return ackResultFailed;
            }
        };
        writer.write_all(CTcp::joinLineFeed(&encode).as_bytes());
        if let Err(e) = writer.flush() {
            println!("send data error, err: {}", e);
            // CTcp::removeConsumer(Arc::clone(&consumers), acks.clone(), &consumer.connUuid);
            return ackResultFailed;
        };
        let recv = match consumer.ackReceiver.recv() {
            Ok(recv) => recv,
            Err(err) => {
                println!("{}", err);
                return ackResultFailed;
            }
        };
        if recv.ackResult == consts::define::ackTrue {
            return ackResultTrue;
        }
        else {
            return ackResultFalse;
        }
    }

    fn joinLineFeed(content: &str) -> String {
        return vec![content, "\n"].join("");
    }

    fn removeConsumer(consumers: Arc<Mutex<HashMap<String, Vec<CConsumerInfo>>>>, acks: Arc<Mutex<HashMap<String, CAckSender>>>, connUuid: &str) {
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
                println!("remove index: {}", index);
            }
        }
        let mut acks = match acks.lock() {
            Ok(acks) => acks,
            Err(_) => return
        };
        acks.remove(connUuid);
    }

    fn notifyConsumer(consumers: Arc<Mutex<HashMap<String, Vec<CConsumerInfo>>>>, queueThreadSet: Arc<Mutex<HashSet<String>>>, sender: Arc<Mutex<mpsc::Sender<CChannelData>>>, dbConn: Arc<Mutex<sqlite3::CSqlite3>>, queueName: &str) {
        let mut consumers = match consumers.lock() {
            Ok(consumers) => consumers,
            Err(_) => return,
        };
        let cons = match consumers.get(queueName) {
            Some(cons) => cons,
            None => {
                return;
            },
        };
        if cons.len() == 0 {
            return;
        }
        let mut set = match queueThreadSet.lock() {
            Ok(set) => set,
            Err(_) => return,
        };
        let sender = match sender.lock() {
            Ok(sender) => sender,
            Err(_) => return,
        };
        match set.get(queueName) {
            Some(queue) => {
                println!("queue alreay exist set");
                return;
            }
            None => {
                // send success -> insert to set
                set.insert(queueName.to_string());
            }
        };
        sender.send(CChannelData{
            queueName: queueName.to_string(),
            dbConn: dbConn,
        });
    }

    fn removeQueueThreadSet(queueThreadSet: Arc<Mutex<HashSet<String>>>, queueName: &str) {
        let mut set = match queueThreadSet.lock() {
            Ok(set) => set,
            Err(_) => return,
        };
        set.remove(queueName);
    }

    fn findConnect(connects: Arc<Mutex<HashMap<String, CConnect>>>, vhost: &str) -> Option<CConnect> {
        return None
    }
}

impl CTcp {
    pub fn new(threadMax: usize) -> CTcp {
        // let (sender, receiver): (mpsc::Sender<CChannelData>, mpsc::Receiver<CChannelData>) = mpsc::channel();
        // let tcp = CTcp{
        //     consumers: HashMap::new(),
        //     acks: HashMap::new(),
        //     queueThreadSet: HashSet::new(),
        //     sender: sender,
        //     receiver: receiver,
        //     threadMax: threadMax,
        //     queuePool: CThreadPool::new(threadMax),
        // };
        // conn
        CTcp{
            connects: HashMap::new(),
            threadMax: threadMax
        }
    }
}

