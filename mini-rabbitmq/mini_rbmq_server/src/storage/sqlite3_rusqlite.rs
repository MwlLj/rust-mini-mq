extern crate rand;
extern crate uuid;

use super::super::consts::exchange;
use rand::Rng;
use uuid::Uuid;

use rusqlite::types::{ToSql};
use rusqlite::{params, Connection, Rows, Row};

pub struct CSqlite3 {
    connect: rusqlite::Result<rusqlite::Connection>
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
            connect: Connection::open(path),
        };
        match storage.createTable() {
            Err(e) => return Ok(storage),
            Ok(s) => s,
        };
        Ok(storage)
    }

    fn createTable(&self) -> rusqlite::Result<()> {
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
            conn.execute(&sql, params![])?;
        }
        Ok(())
    }

    pub fn createExchange(&self, exchangeName: &str, exchangeType: &str) -> rusqlite::Result<()> {
        let count = self.getExchangeCount(exchangeName);
        if count == 0 {
            let sql = format!(
                "
                insert into t_exchange_info values('{}', '{}');
                "
            , exchangeName, exchangeType);
            if let Ok(ref conn) = self.connect {
                conn.execute(&sql, params![])?;
            }
        }
        Ok(())
    }

    pub fn createQueue(&self, queueName: &str, queueType: &str) -> rusqlite::Result<()> {
        let info = self.getQueueByName(queueName);
        if info.count == 0 {
            self.startTransaction();
            let sql = format!(
                "
                insert into t_queue_info values('{}', '{}');
                "
            , queueName, queueType);
            if let Ok(ref conn) = self.connect {
                if let Err(err) = conn.execute(&sql, params![]) {
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
                if let Err(err) = conn.execute(&sql, params![]) {
                    self.rollback();
                    return Err(err);
                }
            }
            self.commit();
        }
        Ok(())
    }

    pub fn createBind(&self, exchangeName: &str, queueName: &str, routerKey: &str) -> rusqlite::Result<()> {
        let count = self.getBindCount(exchangeName, queueName, routerKey);
        if count == 0 {
            let sql = format!(
                "
                insert into t_bind_info values('{}', '{}', '{}');
                "
            , exchangeName, queueName, routerKey);
            if let Ok(ref conn) = self.connect {
                conn.execute(&sql, params![])?;
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
                if let Err(_) = conn.execute(&sql, params![]) {
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
        self.get(&sql, params![]
        , &mut |v: &rusqlite::Row| {
            if let Ok(value) = v.get(0) {
                uuid = value;
            }
            if let Ok(value) = v.get(1) {
                data = value;
            }
            if let Ok(value) = v.get(2) {
                queueType = value;
            }
            if let Ok(value) = v.get(3) {
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
                if let Err(_) = conn.execute(&sql, params![]) {
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
            select count(0) from t_exchange_info where exchange_name = ?1;
            "
        , params![String::from(exchangeName)], &mut |v: &rusqlite::Row| {
            if let Ok(value) = v.get(0) {
                count = value;
            };
        });
        count
    }

    fn getQueueByName(&self, queueName: &str) -> CQueueInfo {
        let mut queueType = String::new();
        let mut count = 0 as u32;
        self.get(
            "
            select queue_type, count(0) from t_queue_info where queue_name = ?1;
            "
        , params![String::from(queueName)], &mut |v: &rusqlite::Row| {
            if let Ok(value) = v.get(0) {
                queueType = value;
            };
            if let Ok(value) = v.get(0) {
                count = value;
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
            where exchange_name = ?1 and queue_name = ?2
            and router_key = ?3;
            "
        , params![String::from(exchangeName)
        , String::from(queueName)
        , String::from(routerKey)], &mut |v: &rusqlite::Row| {
            if let Ok(value) = v.get(0) {
                count = value;
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
            where bi.exchange_name = ?1 and bi.router_key = ?2;
            "
        , params![String::from(exchangeName)
        , String::from(routerKey)], &mut |v: &rusqlite::Row| {
            let mut info = CGetBindInfo::default();
            if let Ok(value) = v.get(0) {
                info.exchangeName = value;
            };
            if let Ok(value) = v.get(1) {
                info.exchangeType = value;
            };
            if let Ok(value) = v.get(2) {
                info.queueName = value;
            };
            if let Ok(value) = v.get(3) {
                info.routerKey = value;
            };
            infos.push(info);
        });
        infos
    }

    fn transaction(&self, sql: &str) -> rusqlite::Result<()> {
        if let Ok(ref conn) = self.connect {
            conn.execute(sql, params![])?;
        }
        Ok(())
    }

    fn startTransaction(&self) -> rusqlite::Result<()> {
        return self.transaction("begin transaction;");
    }

    fn commit(&self) -> rusqlite::Result<()> {
        return self.transaction("commit;");
    }

    fn rollback(&self) -> rusqlite::Result<()> {
        return self.transaction("rollback;");
    }
}

impl CSqlite3 {
    fn get<Func>(&self, sql: &str, params: &[&dyn ToSql], callback: &mut Func)
        where Func: FnMut(&rusqlite::Row) {
        let conn = match self.connect {
            Ok(ref conn) => conn,
            Err(_) => return,
        };
        let mut pre = match conn.prepare(sql) {
            Ok(pre) => pre,
            Err(err) => {
                println!("prepare error, err: {}", err);
                return;
            }
        };
        let rows = pre.query(params);
        let mut rows = match rows {
            Ok(rows) => rows,
            Err(_) => return,
        };
        while let Ok(next) = rows.next() {
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

