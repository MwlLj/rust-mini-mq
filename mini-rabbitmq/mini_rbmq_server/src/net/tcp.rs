// extern crate serde_json;
extern crate rustc_serialize;

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

use super::super::pool::thread::CThreadPool;
use super::super::storage::IStorage;
use super::super::storage::file::CFile;
use super::super::statics::content::CContent;

const requestModeConnect: &str = "connect";
const requestModeSending: &str = "sending";
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
    serverName: String,
    serverVersion: String,
    serverNo: String,
    topic: String,
    data: String,
    storageMode: String,
    logType: String
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
    queuePool: CThreadPool,
    storageFile: CFile,
    content: CContent
}

impl CConnect {
    fn joinKey(serverName: String, serverVersion: String, serverNo: String) -> String {
    	if serverName != "" && serverVersion == "" && serverNo == "" {
    		// to do
    	}
        let key = vec![serverName, serverVersion, serverNo].join("-");
        key
    }

    pub fn start(self, addr: &str) {
        let listener = TcpListener::bind(addr).unwrap();
        let subscribes = Arc::new(Mutex::new(self.subscribes));
        let threadPool = Arc::new(Mutex::new(self.queuePool));
        let storageFile = Arc::new(Mutex::new(self.storageFile));
        let contentStatic = Arc::new(Mutex::new(self.content));
        for stream in listener.incoming() {
            let subscribes = subscribes.clone();
            let threadPool = threadPool.clone();
            let storageFile = storageFile.clone();
            let contentStatic = contentStatic.clone();
            thread::spawn(move || {
                if let Ok(stream) = stream {
                    let mut reader = BufReader::new(&stream);
                    // loop {
                    //     let mut buf = vec![];
                    //     if let Err(_) = reader.read_until(b'\n', &mut buf) {
                    //         break;
                    //     }
                        // let body = String::from_utf8(buf);
                        // if let Ok(request) = json::decode(body.unwrap().as_str()) {
                    for line in reader.lines() {
                        if let Ok(line) = line {
                            if let Ok(request) = json::decode(&line) {
                                let request: CRequest = request;
                                if request.mode == requestModeConnect && request.identify == requestIdentifyPublish {
                                    let key = CConnect::joinKey(request.serverName, request.serverVersion, request.serverNo);
                                    // create subscribes map
                                    let mut subs = subscribes.lock().unwrap();
                                    match subs.get_mut(&key) {
                                        None => {
                                            subs.insert(key, Vec::new());
                                        },
                                        Some(_) => {}
                                    }
                                } else if request.mode == requestModeSending && request.identify == requestIdentifyPublish {
                                    // handle server send data
                                    let key = CConnect::joinKey(request.serverName.clone(), request.serverVersion.clone(), request.serverNo.clone());
                                    // broadcast in thread pool
                                    let pool = threadPool.lock().unwrap();
                                    let subscribes = subscribes.clone();
                                    let storageFile = storageFile.clone();
                                    let contentStatic = contentStatic.clone();
                                    pool.execute(move || {
                                        let mut subs = subscribes.lock().unwrap();
                                        let sf = storageFile.lock().unwrap();
                                        let cs = contentStatic.lock().unwrap();
                                        if let Some(subQueue) = subs.get_mut(&key) {
                                            let mut removes = Vec::new();
                                            let mut index = 0;
                                            let content = vec![request.data.clone(), "\n".to_string()].join("");
                                            let content = cs.full(&key, &request.logType, &request.topic, &content);
                                            if request.storageMode == storageModeFile {
                                                sf.write(&key, &request.logType, &content);
                                                sf.write(&key, "full", &content);
                                                // if cfg!(all(target_os="linux", target_arch="arm")) {
                                                // } else {
                                                //     sf.write(&key, &request.logType, &content);
                                                // }
                                            }
                                            for sub in &(*subQueue) {
                                            	let mut isSend = false;
                                            	if (sub.topic != "" && sub.logType == "") && request.topic != sub.topic {
                                            		isSend = false;
                                            	} else if (sub.logType != "" && sub.topic == "") && request.logType != sub.logType {
                                            		isSend = false;
                                            	} else if (sub.logType != "" && sub.topic != "") && (request.logType != sub.logType || request.topic != sub.topic) {
                                            		isSend = false;
                                            	} else {
                                            		isSend = true;
            	                                }
                                                let mut writer = BufWriter::new(&sub.stream);
                                                if isSend {
            	                                    writer.write_all(content.as_bytes());
            	                                } else {
            	                                	writer.write_all(b"\n");
            	                                }
                                                if let Err(e) = writer.flush() {
                                                    // (*subQueue).remove_item(sub);
                                                    removes.push(index);
                                                }
                                                index += 1;
                                            }
                                            for removeIndex in removes {
                                                println!("remove index: {}", removeIndex);
                                                (*subQueue).remove(removeIndex);
                                            }
                                        };
                                    });
                                } else if request.mode == requestModeConnect && request.identify == requestIdentifySubscribe {
                                    let key = CConnect::joinKey(request.serverName.clone(), request.serverVersion.clone(), request.serverNo.clone());
                                    let mut subs = subscribes.lock().unwrap();
                                    let sub = CSubscribeInfo {
                                        stream: stream,
                                        topic: request.topic,
                                        logType: request.logType,
                                        serverName: request.serverName,
                                        serverVersion: request.serverVersion,
                                        serverNo: request.serverNo
                                    };
                                    match subs.get_mut(&key) {
                                        Some(value) => {
                                            (*value).push(sub);
                                        },
                                        None => {
                                            let mut v = Vec::new();
                                            v.push(sub);
                                            subs.insert(key, v);
                                        }
                                    };
                                    break;
                                }
                            }
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
            storageFile: CFile::new(),
            content: CContent::new()
        };
        conn
    }
}

