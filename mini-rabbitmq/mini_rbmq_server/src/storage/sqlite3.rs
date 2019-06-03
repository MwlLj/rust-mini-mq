extern crate sqlite3;

pub struct CSqlite3 {
    connect: sqlite3::Result<sqlite3::Connection>
}

struct CGetBindInfoByExchangeRouterKeyOutput {
}

impl CSqlite3 {
    pub fn connect(vhost: &str) -> Result<CSqlite3, &str> {
        let mut path = String::from(vhost);
        path.push_str(".db");
        let storage = CSqlite3{
            connect: sqlite3::open(path),
        };
        match storage.createTable() {
            Err(e) => return Err("create table error"),
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
            create table if not exists t_queue_info (
                queue_name varchar(64) primary key
            );
            create table if not exists t_bind_info (
                exchange_name varchar(64),
                queue_name varchar(64),
                router_key varchar(64)
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

    pub fn createQueue(&self, queueName: &str) -> sqlite3::Result<()> {
        let sql = format!(
            "
            insert into t_queue_info values('{}');
            "
        , queueName);
        if let Ok(ref conn) = self.connect {
            conn.execute(sql)?
        }
        Ok(())
    }

    pub fn createBind(&self, exchangeName: &str, queueName: &str, routerKey: &str) -> sqlite3::Result<()> {
        let sql = format!(
            "
            insert into t_bind_info values({}, {}, {});
            "
        , exchangeName, queueName, routerKey);
        if let Ok(ref conn) = self.connect {
            conn.execute(sql)?
        }
        Ok(())
    }

    fn getBindInfoByExchangeRouterKey(&self, exchangeName: &str, routerKey: &str) -> sqlite3::Result<()> {
        let sql = format!(
            "
            select exchange_name, queue_name, router_key, count(0) from t_bind_info
            where exchange_name = {} and router_key = {};
            "
        , exchangeName, routerKey);
        if let Ok(ref conn) = self.connect {
            conn.iterate(sql, |pairs| {
                for &(col, value) in pairs.iter() {
                }
                true
            })?
        }
        Ok(())
    }

    pub fn addData(&self, exchangeName: &str, routerKey: &str) -> sqlite3::Result<()> {
        let sql = format!(
            "
            "
        );
        if let Ok(ref conn) = self.connect {
            conn.execute(sql)?
        }
        Ok(())
    }
}

impl CSqlite3 {
    fn getExchangeCount(&self, exchangeName: &str) -> u32 {
        let mut count = 0 as u32;
        self.getSingle(
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
}

impl CSqlite3 {
    fn getSingle<Func>(&self, sql: &str, params: &[sqlite3::Value], callback: &mut Func)
        where Func: FnMut(&[sqlite3::Value]) {
        let conn = match self.connect {
            Ok(ref conn) => conn,
            Err(_) => return,
        };
        let pre = match conn.prepare(sql) {
            Ok(pre) => pre,
            Err(_) => return,
        };
        let mut cursor = pre.cursor();
        if let Err(_) = cursor.bind(params) {
            return;
        }
        let next = match cursor.next() {
            Ok(next) => next,
            Err(_) => return,
        };
        if let Some(row) = next {
            callback(row);
        }
    }
}

