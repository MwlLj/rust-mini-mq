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
use std::path::Path;
use std::path::PathBuf;
use std::fs::DirBuilder;

use rustc_serialize::json;

use rand::Rng;

use rust_pool::thread::simple::CThreadPool;
use super::super::storage::sqlite3;
use super::super::consts;
use super::super::decode;

use uuid::Uuid;

const requestModeConnect: &str = "connect";
const requestModeCreateExchange: &str = "create-exchange";
const requestModeCreateQueue: &str = "create-queue";
const requestModeCreateBind: &str = "create-bind";
const requestModeClearQueue: &str = "clear-queue";
const requestModePublish: &str = "publish";
const requestModeConsumer: &str = "consumer";
const requestModeTrigConsumer: &str = "trig-consumer";
const requestModeAck: &str = "ack";

const responseModeResult: &str = "result";
const responseModeConnect: &str = "connect";
const responseModeData: &str = "data";

const ackResultSuccess: u32 = 0;
const ackResultFailed: u32 = 1;
const ackResultTrue: u32 = 2;
const ackResultFalse: u32 = 3;

macro_rules! decode_request {
    ($index:ident, $s:ident, $req:ident) => ({
        if $index % 2 == 0 {
            let two: u32 = 2;
            let mut number = 0;
            let mut i = 0;
            for item in $s {
                // println!("{}, {}, {}", item, i, two.pow(i));
                number += item as u32 * two.pow(i);
                i += 1;
            }
            return (true, number);
        }
        if $index == 1 {$req.mode = String::from_utf8($s).unwrap()}
        else if $index == 3 {$req.identify = String::from_utf8($s).unwrap()}
        else if $index == 5 {$req.vhost = String::from_utf8($s).unwrap()}
        else if $index == 7 {$req.exchangeName = String::from_utf8($s).unwrap()}
        else if $index == 9 {$req.exchangeType = String::from_utf8($s).unwrap()}
        else if $index == 11 {$req.queueName = String::from_utf8($s).unwrap()}
        else if $index == 13 {$req.queueType = String::from_utf8($s).unwrap()}
        else if $index == 15 {$req.routerKey = String::from_utf8($s).unwrap()}
        else if $index == 17 {$req.data = String::from_utf8($s).unwrap()}
        else if $index == 19 {$req.ackResult = String::from_utf8($s).unwrap()}
        else if $index == 21 {$req.messageNo = String::from_utf8($s).unwrap()}
        if $index == 21 {
            return (false, 0);
        }
        return (true, 32);
    })
}

// #[derive(Serialize, Deserialize)]
#[derive(RustcDecodable, RustcEncodable, Default)]
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

#[derive(RustcDecodable, RustcEncodable)]
pub struct CResponse {
    mode: String,
    queueName: String,
    data: String,
    messageNo: String,
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
    connects: HashMap<String, Arc<Mutex<CConnect>>>,
    threadMax: usize
}

pub struct CConnect {
    consumers: Arc<Mutex<HashMap<String, Vec<CConsumerInfo>>>>,
    queueThreadSet: Arc<Mutex<HashSet<String>>>,
    sender: Arc<Mutex<mpsc::Sender<CChannelData>>>,
    receiver: Arc<Mutex<mpsc::Receiver<CChannelData>>>
}

pub struct CChannelData {
    queueName: String,
    dbConn: Arc<Mutex<sqlite3::CSqlite3>>
}

impl CTcp {
    pub fn start(self, addr: &str, storageRoot: String) {
        CTcp::createDir(&storageRoot);
        let listener = TcpListener::bind(addr).unwrap();
        let connects = Arc::new(Mutex::new(self.connects));
        let threadMax = Arc::new(Mutex::new(self.threadMax));
        let storageRoot = Arc::new(Mutex::new(storageRoot));
        for stream in listener.incoming() {
            let connects = connects.clone();
            let storageRoot = storageRoot.clone();
            let tm = threadMax.clone();
            thread::spawn(move || {
                let connUuid = Uuid::new_v4();
                if let Ok(stream) = stream {
                    let mut vhost = String::new();
                    {
                        let mut request = CRequest::default();
                        let mut r = decode::stream::CStreamBlockParse::new(stream.try_clone().unwrap());
                        r.line(32, &mut request, &mut |index: u64, buf: Vec<u8>, req: &mut CRequest| -> (bool, u32) {
                            decode_request!(index, buf, req);
                        });
                        vhost = request.vhost;
                    }
                    println!("vhost: {}", &vhost);
                    let mut threadMax: Option<usize> = None;
                    {
                        let tm = match tm.try_lock() {
                            Ok(tm) => tm,
                            Err(err) => {
                                println!("{:?}", err);
                                return;
                            },
                        };
                        threadMax = Some(*tm);
                    }
                    let threadMax = match threadMax {
                        Some(max) => max,
                        None => return,
                    };
                    let mut root: Option<String> = None;
                    {
                        let rt = match storageRoot.try_lock() {
                            Ok(rt) => rt,
                            Err(err) => {
                                println!("{:?}", err);
                                return;
                            },
                        };
                        root = Some(rt.clone());
                    }
                    let root = match root {
                        Some(root) => root,
                        None => return,
                    };
                    let dbConnect = Arc::new(Mutex::new(match sqlite3::CSqlite3::connect(&CTcp::joinStoragePath(&root, &vhost)) {
                        Ok(conn) => conn,
                        Err(err) => {
                            println!("{:?}", err);
                            return
                        }
                    }));
                    let mut connect: Option<Arc<Mutex<CConnect>>> = None;
                    let mut isFindConnect = true;
                    {
                        let mut connects = match connects.try_lock() {
                            Ok(connects) => connects,
                            Err(_) => {
                                println!("connects try clone error");
                                return;
                            }
                        };
                        let mut conn = match connects.get_mut(&vhost) {
                            Some(conn) => conn.clone(),
                            None => {
                                isFindConnect = false;
                                let (sender, receiver): (mpsc::Sender<CChannelData>, mpsc::Receiver<CChannelData>) = mpsc::channel();
                                let conn = Arc::new(Mutex::new(CConnect{
                                    consumers: Arc::new(Mutex::new(HashMap::new())),
                                    queueThreadSet: Arc::new(Mutex::new(HashSet::new())),
                                    sender: Arc::new(Mutex::new(sender)),
                                    receiver: Arc::new(Mutex::new(receiver))
                                }));
                                connects.insert(vhost.to_string(), conn.clone());
                                conn
                            },
                        };
                        connect = Some(conn.clone());
                    }
                    let connect = match connect {
                        Some(connect) => connect,
                        None => return,
                    };
                    let connect = match connect.try_lock() {
                        Ok(connect) => connect,
                        Err(_) => {
                            println!("connect try clone error");
                            return;
                        },
                    };
                    let consumers = connect.consumers.clone();
                    let queueThreadSet = connect.queueThreadSet.clone();
                    let sender = connect.sender.clone();
                    let receiver = connect.receiver.clone();
                    if isFindConnect == false {
                        println!("not found connect, vhost = {}", &vhost);
                        CTcp::dispatch(
                            consumers.clone(),
                            queueThreadSet.clone(),
                            threadMax,
                            receiver.clone());
                    }
                    thread::spawn(move || {
                        println!("send connect response");
                        // response connect success
                        {
                            let mut writer = BufWriter::new(&stream);
                            let res = CResponse{
                                mode: responseModeConnect.to_string(),
                                queueName: "".to_string(),
                                data: "".to_string(),
                                messageNo: "".to_string(),
                                error: consts::result::resultOkError,
                                errorString: consts::result::resultOkErrorString.to_string()
                            };
                            if !CTcp::sendResponse(stream.try_clone().unwrap(), res) {
                                return;
                            };
                        }
                        let mut req = CRequest::default();
                        let mut r = decode::stream::CStreamBlockParse::new(stream.try_clone().unwrap());
                        r.lines(32, &mut req, &mut |index: u64, buf: Vec<u8>, request: &mut CRequest| -> (bool, u32) {
                            decode_request!(index, buf, request);
                        }, |request: &CRequest| -> bool {
                            let mut error: u32 = consts::result::resultOkError;
                            let mut errorString: String = consts::result::resultOkErrorString.to_string();
                            loop {
                                if request.mode == requestModeCreateExchange {
                                    // create exchange
                                    let dbConn = match dbConnect.lock() {
                                        Ok(dbConn) => dbConn,
                                        Err(_) => {
                                            error = consts::code::lock_error;
                                            errorString = consts::code::error_string(error);
                                            break;
                                        }
                                    };
                                    if let Err(_) = dbConn.createExchange(&request.exchangeName, &request.exchangeType) {
                                        error = consts::code::db_error;
                                        errorString = consts::code::error_string(error);
                                        break;
                                    }
                                } else if request.mode == requestModeCreateQueue {
                                    // create queue
                                    let dbConn = match dbConnect.lock() {
                                        Ok(dbConn) => dbConn,
                                        Err(_) => {
                                            error = consts::code::lock_error;
                                            errorString = consts::code::error_string(error);
                                            break;
                                        }
                                    };
                                    if let Err(_) = dbConn.createQueue(&request.queueName, &request.queueType) {
                                        error = consts::code::db_error;
                                        errorString = consts::code::error_string(error);
                                        break;
                                    }
                                } else if request.mode == requestModeClearQueue {
                                    let dbConn = match dbConnect.lock() {
                                        Ok(dbConn) => dbConn,
                                        Err(_) => {
                                            error = consts::code::lock_error;
                                            errorString = consts::code::error_string(error);
                                            break;
                                        }
                                    };
                                    if let Err(_) = dbConn.deleteQueue(&request.queueName) {
                                        error = consts::code::db_error;
                                        errorString = consts::code::error_string(error);
                                        break;
                                    }
                                } else if request.mode == requestModeCreateBind {
                                    // create bind
                                    let dbConn = match dbConnect.lock() {
                                        Ok(dbConn) => dbConn,
                                        Err(_) => {
                                            error = consts::code::lock_error;
                                            errorString = consts::code::error_string(error);
                                            break;
                                        }
                                    };
                                    if let Err(_) = dbConn.createBind(&request.exchangeName, &request.queueName, &request.routerKey) {
                                        error = consts::code::db_error;
                                        errorString = consts::code::error_string(error);
                                        break;
                                    }
                                } else if request.mode == requestModePublish {
                                    // create bind
                                    let dbConn = match dbConnect.lock() {
                                        Ok(dbConn) => dbConn,
                                        Err(_) => {
                                            error = consts::code::lock_error;
                                            errorString = consts::code::error_string(error);
                                            break;
                                        }
                                    };
                                    if let Ok(queues) = dbConn.addData(&request.exchangeName, &request.routerKey, &request.data) {
                                        for queue in queues {
                                            CTcp::notifyConsumer(consumers.clone(), queueThreadSet.clone(), sender.clone(), dbConnect.clone(), &queue);
                                        }
                                    } else {
                                        error = consts::code::db_error;
                                        errorString = consts::code::error_string(error);
                                        break;
                                    }
                                } else if request.mode == requestModeConsumer {
                                    // consumer
                                    {
                                        let mut consumes = match consumers.lock() {
                                            Ok(consumes) => consumes,
                                            Err(_) => {
                                                error = consts::code::lock_error;
                                                errorString = consts::code::error_string(error);
                                                break;
                                            }
                                        };
                                        let stream = match stream.try_clone() {
                                            Ok(stream) => stream,
                                            Err(_) => {
                                                error = consts::code::lock_error;
                                                errorString = consts::code::error_string(error);
                                                break
                                            }
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
                                    }
                                    CTcp::notifyConsumer(consumers.clone(), queueThreadSet.clone(), sender.clone(), dbConnect.clone(), &request.queueName);
                                } else if request.mode == requestModeAck {
                                    {
                                        let dbConn = match dbConnect.lock() {
                                            Ok(dbConn) => dbConn,
                                            Err(_) => {
                                                error = consts::code::lock_error;
                                                errorString = consts::code::error_string(error);
                                                break;
                                            }
                                        };
                                        if request.ackResult == consts::define::ackTrue {
                                            if let Err(err) = dbConn.deleteQueueData(&request.queueName, &request.messageNo) {
                                                println!("delete data error, err: {}", err);
                                                error = consts::code::db_error;
                                                errorString = consts::code::error_string(error);
                                                break;
                                            };
                                        }
                                    }
                                    if request.ackResult == consts::define::ackTrue {
                                        CTcp::notifyConsumer(consumers.clone(), queueThreadSet.clone(), sender.clone(), dbConnect.clone(), &request.queueName);
                                    }
                                    /*
                                    // ack
                                    let mut acks = match acks.try_lock() {
                                        Ok(acks) => acks,
                                        Err(_) => {
                                            error = consts::code::lock_error;
                                            errorString = consts::code::error_string(error);
                                            println!("try lock error");
                                            break;
                                        }
                                    };
                                    if let Some(send) = acks.get(&connUuid.to_string()) {
                                        if let Err(err) = send.ackSender.send(CAckInfo{
                                            ackResult: request.ackResult.to_string(),
                                        }) {
                                            error = consts::code::send_error;
                                            errorString = consts::code::error_string(error);
                                            println!("ackSender send error: {}", err);
                                        }
                                    }
                                    */
                                } else if request.mode == requestModeTrigConsumer {
                                    CTcp::notifyConsumer(consumers.clone(), queueThreadSet.clone(), sender.clone(), dbConnect.clone(), &request.queueName);
                                }
                                break
                            }
                            let res = CResponse{
                                mode: responseModeResult.to_string(),
                                queueName: "".to_string(),
                                data: "".to_string(),
                                messageNo: request.messageNo.clone(),
                                error: error,
                                errorString: errorString,
                            };
                            if !CTcp::sendResponse(stream.try_clone().unwrap(), res) {
                                return false;
                            };
                            return true;
                        });
                        {
                            // disconnect handle
                            println!("remove consumer");
                            CTcp::removeConsumer(consumers.clone(), &connUuid.to_string());
                        }
                    });
                }
            });
        }
    }
}

// private
impl CTcp {
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
                    if let Some(data) = dbConn.getOneData(&recv.queueName, |queueType: &str, dataUuid: &str, data: &str| {
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
                                let r = CTcp::sendToConsumer(consumers.clone(), &consumer, &recv.queueName, dataUuid, data);
                                if r == ackResultFalse || r == ackResultFailed {
                                    println!("error 1");
                                    return false;
                                }
                            }
                        } else if queueType == consts::queue::queueTypeFanout {
                            // send to all consumer
                            for consumer in consumersList {
                                let r = CTcp::sendToConsumer(consumers.clone(), &consumer, &recv.queueName, dataUuid, data);
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

    fn sendToConsumer(consumers: Arc<Mutex<HashMap<String, Vec<CConsumerInfo>>>>, consumer: &CConsumerInfo, queueName: &str, dataUuid: &str, data: &str) -> u32 {
        let response = CResponse{
            mode: responseModeData.to_string(),
            queueName: queueName.to_string(),
            data: data.to_string(),
            // use messageNo replace dataUuid
            messageNo: dataUuid.to_string(),
            error: consts::result::resultOkError,
            errorString: consts::result::resultOkErrorString.to_string()
        };
        println!("send data");
        if !CTcp::sendResponse(consumer.stream.try_clone().unwrap(), response) {
            println!("send data error");
            // CTcp::removeConsumer(Arc::clone(&consumers), acks.clone(), &consumer.connUuid);
            return ackResultFailed;
        };
        return ackResultTrue;
        /*
        println!("wait recv ack");
        let recv = match consumer.ackReceiver.recv() {
            Ok(recv) => recv,
            Err(err) => {
                println!("{}", err);
                return ackResultFailed;
            }
        };
        println!("recv ack");
        if recv.ackResult == consts::define::ackTrue {
            return ackResultTrue;
        }
        else {
            return ackResultFalse;
        }
        */
    }

    fn append32Number(value: u32, buf: &mut Vec<u8>) {
        for i in 0..32 {
            let b = (value >> i) & 1;
            buf.push(b as u8);
        }
    }

    fn sendResponse(stream: TcpStream, response: CResponse) -> bool {
        let mut writer = BufWriter::new(&stream);
        let mut buf = Vec::new();
        CTcp::append32Number(response.mode.len() as u32, &mut buf);
        buf.append(&mut response.mode.as_bytes().to_vec());
        CTcp::append32Number(response.queueName.len() as u32, &mut buf);
        buf.append(&mut response.queueName.as_bytes().to_vec());
        CTcp::append32Number(response.data.len() as u32, &mut buf);
        buf.append(&mut response.data.as_bytes().to_vec());
        CTcp::append32Number(response.messageNo.len() as u32, &mut buf);
        buf.append(&mut response.messageNo.as_bytes().to_vec());
        let errorStr = response.error.to_string();
        CTcp::append32Number(errorStr.len() as u32, &mut buf);
        buf.append(&mut errorStr.as_bytes().to_vec());
        CTcp::append32Number(response.errorString.len() as u32, &mut buf);
        buf.append(&mut response.errorString.as_bytes().to_vec());
        if let Err(err) = writer.write_all(&buf) {
            return false;
        };
        if let Err(err) = writer.flush() {
            return false;
        };
        true
    }

    fn createDir(root: &str) {
        let full = Path::new(root);
        if full.exists() {
            return;
        }
        if let Ok(_) = DirBuilder::new().recursive(true).create(&full) {
            return;
        }
    }

    fn joinLineFeed(content: &str) -> String {
        return vec![content, "\n"].join("");
    }

    fn joinStoragePath(storageRoot: &str, path: &str) -> String {
        let mut root = String::from(storageRoot);
        root.push_str("/");
        root.push_str(path);
        root
    }

    fn removeConsumer(consumers: Arc<Mutex<HashMap<String, Vec<CConsumerInfo>>>>, connUuid: &str) {
        let mut consumers = match consumers.lock() {
            Ok(cons) => cons,
            Err(_) => {
                // CTcp::removeAck(acks.clone(), connUuid);
                return;
            }
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
        // CTcp::removeAck(acks.clone(), connUuid);
        /*
        let mut acks = match acks.lock() {
            Ok(acks) => {
                println!("1");
                if let Some(send) = acks.get(connUuid) {
                    println!("2");
                    send.ackSender.send(CAckInfo{
                        ackResult: consts::define::ackFalse.to_string(),
                    });
                    println!("3");
                };
                acks
            },
            Err(_) => return
        };
        acks.remove(connUuid);
        */
    }

    fn removeAck(acks: Arc<Mutex<HashMap<String, CAckSender>>>, connUuid: &str) {
        let mut acks = match acks.lock() {
            Ok(acks) => {
                if let Some(send) = acks.get(connUuid) {
                    send.ackSender.send(CAckInfo{
                        ackResult: consts::define::ackFalse.to_string(),
                    });
                };
                acks
            },
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
}

impl CTcp {
    pub fn new(threadMax: usize) -> CTcp {
        CTcp{
            connects: HashMap::new(),
            threadMax: threadMax
        }
    }
}

