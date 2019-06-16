extern crate sqlite3;
extern crate rand;
extern crate uuid;

use super::super::consts::exchange;
use rand::Rng;
use uuid::Uuid;

pub struct CSqlite3 {
    connect: sqlite3::Result<sqlite3::Connection>
}

#[derive(Default, Debug)]
struct CGetBindInfo {
    exchangeName: String,
    exchangeType: String,
    queueName: String,
    routerKey: String
}

struct CQueueInfo {
    queueType: String,
    count: u32
}

impl CSqlite3 {
    pub fn connect(vhost: &str) -> Result<CSqlite3, &str> {
        let mut path = String::from(vhost);
        path.push_str(".db");
        let storage = CSqlite3{
            connect: sqlite3::open(path),
        };
        match storage.createTable() {
            Err(e) => return Ok(storage),
            Ok(s) => s,
        };
        Ok(storage)
    }

    fn createTable(&self) -> sqlite3::Result<()> {
        let sql = format!(
            "
            create table if not exists t_exchange_info (
                exchange_name varchar(64) primary key,
                exchange_type varchar(64)
            );
            create table if not exists t_bind_info (
                exchange_name varchar(64),
                queue_name varchar(64),
                router_key varchar(64)
            );
            create table if not exists t_queue_info (
                queue_name varchar(64),
                queue_type varchar(64)
            );
            "
        );
        if let Ok(ref conn) = self.connect {
            conn.execute(sql)?
        }
        Ok(())
    }

    pub fn createExchange(&self, exchangeName: &str, exchangeType: &str) -> sqlite3::Result<()> {
        let count = self.getExchangeCount(exchangeName);
        if count == 0 {
            let sql = format!(
                "
                insert into t_exchange_info values('{}', '{}');
                "
            , exchangeName, exchangeType);
            if let Ok(ref conn) = self.connect {
                conn.execute(sql)?
            }
        }
        Ok(())
    }

    pub fn createQueue(&self, queueName: &str, queueType: &str) -> sqlite3::Result<()> {
        let info = self.getQueueByName(queueName);
        if info.count == 0 {
            self.startTransaction();
            let sql = format!(
                "
                insert into t_queue_info values('{}', '{}');
                "
            , queueName, queueType);
            if let Ok(ref conn) = self.connect {
                if let Err(err) = conn.execute(sql) {
                    self.rollback();
                    return Err(err);
                }
            }
            let sql = format!(
                "
                create table if not exists {} (
                    uuid varchar(64),
                    data text
                );
                "
            , queueName);
            if let Ok(ref conn) = self.connect {
                if let Err(err) = conn.execute(sql) {
                    self.rollback();
                    return Err(err);
                }
            }
            self.commit();
        }
        Ok(())
    }

    pub fn createBind(&self, exchangeName: &str, queueName: &str, routerKey: &str) -> sqlite3::Result<()> {
        let count = self.getBindCount(exchangeName, queueName, routerKey);
        if count == 0 {
            let sql = format!(
                "
                insert into t_bind_info values('{}', '{}', '{}');
                "
            , exchangeName, queueName, routerKey);
            if let Ok(ref conn) = self.connect {
                conn.execute(sql)?
            }
        }
        Ok(())
    }

    pub fn addData(&self, exchangeName: &str, routerKey: &str, data: &str) -> Result<Vec<String>, &str> {
        let mut infos = self.getBindInfoByExchangeRouterKey(exchangeName, routerKey);
        let length = infos.len();
        if length == 0 {
            println!("no queue be bind");
            return Err("no queue be bind");
        }
        let mut queues = Vec::new();
        for info in &infos {
            queues.push(info.queueName.to_string());
        }
        let first = &infos[0];
        if first.exchangeType == exchange::exchangeTypeDirect {
            // random
            let index = rand::thread_rng().gen_range(0, length);
            for i in 0..length {
                if i != index {
                    infos.remove(i);
                }
            }
        } else if first.exchangeType == exchange::exchangeTypeFanout {
            // all
        }
        self.startTransaction();
        let mut result = true;
        for info in infos {
            let uid = uuid::Uuid::new_v4();
            let sql = format!(
                "
                insert into {} values('{}', '{}');
                "
            , &info.queueName, uid, data);
            if let Ok(ref conn) = self.connect {
                if let Err(_) = conn.execute(sql) {
                    result = false;
                    break;
                }
            } else {
                result = false;
                break;
            }
        }
        if result {
            self.commit();
            Ok(queues)
        } else {
            self.rollback();
            Err("inner error")
        }
    }

    pub fn getOneData<Func>(&self, queueName: &str, callback: Func) -> Option<String>
        where Func: Fn(&str, &str) -> bool {
        let mut uuid = String::new();
        let mut data = String::new();
        let mut queueType = String::new();
        let mut count: i64 = 0;
        let sql = format!(
            "
            select q.uuid, q.data, tqi.queue_type, count(0) from {} as q, t_queue_info as tqi where tqi.queue_name = '{}' limit 1;
            "
            , queueName, queueName);
        self.get(&sql, &[]
        , &mut |v: &[sqlite3::Value]| {
            if let Some(value) = v[0].as_string() {
                uuid = value.to_string();
            }
            if let Some(value) = v[1].as_string() {
                data = value.to_string();
            }
            if let Some(value) = v[2].as_string() {
                queueType = value.to_string();
            }
            if let Some(value) = v[3].as_integer() {
                count = value;
            }
        });
        if count == 0 {
            return None;
        }
        let result = callback(&queueType, &data);
        if result {
            let sql = format!(
                "
                delete from {} where uuid = '{}';
                "
            , queueName, uuid);
            if let Ok(ref conn) = self.connect {
                if let Err(_) = conn.execute(sql) {
                    return None;
                }
            }
        } else {
            return None;
        }
        if count == 0 {
            None
        } else {
            Some(data)
        }
    }
}

impl CSqlite3 {
    fn getExchangeCount(&self, exchangeName: &str) -> u32 {
        let mut count = 0 as u32;
        self.get(
            "
            select count(0) from t_exchange_info where exchange_name = ?;
            "
        , &[sqlite3::Value::String(String::from(exchangeName))], &mut |v: &[sqlite3::Value]| {
            if let Some(value) = v[0].as_integer() {
                count = value as u32;
            };
        });
        count
    }

    fn getQueueByName(&self, queueName: &str) -> CQueueInfo {
        let mut queueType = String::new();
        let mut count = 0 as u32;
        self.get(
            "
            select queue_type, count(0) from t_queue_info where queue_name = ?;
            "
        , &[sqlite3::Value::String(String::from(queueName))], &mut |v: &[sqlite3::Value]| {
            if let Some(value) = v[0].as_string() {
                queueType = value.to_string();
            };
            if let Some(value) = v[1].as_integer() {
                count = value as u32;
            };
        });
        CQueueInfo{
            queueType: queueType,
            count: count,
        }
    }

    fn getBindCount(&self, exchangeName: &str, queueName: &str, routerKey: &str) -> u32 {
        let mut count = 0 as u32;
        self.get(
            "
            select count(0) from t_bind_info
            where exchange_name = ? and queue_name = ?
            and router_key = ?;
            "
        , &[sqlite3::Value::String(String::from(exchangeName))
        , sqlite3::Value::String(String::from(queueName))
        , sqlite3::Value::String(String::from(routerKey))], &mut |v: &[sqlite3::Value]| {
            if let Some(value) = v[0].as_integer() {
                count = value as u32;
            };
        });
        count
    }

    fn getBindInfoByExchangeRouterKey(&self, exchangeName: &str, routerKey: &str) -> Vec<CGetBindInfo> {
        // println!("getBindInfoByExchangeRoterKey");
        let mut infos: Vec<CGetBindInfo> = Vec::new();
        self.get(
            "
            select bi.exchange_name, ei.exchange_type
            , bi.queue_name, bi.router_key
            from t_bind_info as bi
            inner join t_exchange_info as ei
            on bi.exchange_name = ei.exchange_name
            where bi.exchange_name = ? and bi.router_key = ?;
            "
        , &[sqlite3::Value::String(String::from(exchangeName))
        , sqlite3::Value::String(String::from(routerKey))], &mut |v: &[sqlite3::Value]| {
            let mut info = CGetBindInfo::default();
            if let Some(value) = v[0].as_string() {
                info.exchangeName = value.to_string();
            };
            if let Some(value) = v[1].as_string() {
                info.exchangeType = value.to_string();
            };
            if let Some(value) = v[2].as_string() {
                info.queueName = value.to_string();
            };
            if let Some(value) = v[3].as_string() {
                info.routerKey = value.to_string();
            };
            infos.push(info);
        });
        infos
    }

    fn transaction(&self, sql: &str) -> sqlite3::Result<()> {
        if let Ok(ref conn) = self.connect {
            conn.execute(sql)?
        }
        Ok(())
    }

    fn startTransaction(&self) -> sqlite3::Result<()> {
        return self.transaction("begin transaction;");
    }

    fn commit(&self) -> sqlite3::Result<()> {
        return self.transaction("commit;");
    }

    fn rollback(&self) -> sqlite3::Result<()> {
        return self.transaction("rollback;");
    }
}

impl CSqlite3 {
    fn get<Func>(&self, sql: &str, params: &[sqlite3::Value], callback: &mut Func)
        where Func: FnMut(&[sqlite3::Value]) {
        let conn = match self.connect {
            Ok(ref conn) => conn,
            Err(_) => return,
        };
        let pre = match conn.prepare(sql) {
            Ok(pre) => pre,
            Err(err) => {
                println!("prepare error, err: {}", err);
                return;
            }
        };
        let mut cursor = pre.cursor();
        if let Err(err) = cursor.bind(params) {
            println!("param bind error, err: {}", err);
            return;
        }
        while let Ok(next) = cursor.next() {
            if let Some(row) = next {
                callback(row);
            } else {
                break;
            }
        }
        // let next = match cursor.next() {
        //     Ok(next) => next,
        //     Err(_) => return,
        // };
        // if let Some(row) = next {
        //     callback(row);
        // }
    }
}

