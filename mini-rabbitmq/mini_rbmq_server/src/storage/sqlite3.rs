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
                queue_name varchar(64) primary key,
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
        let sql = format!(
            "
            insert into t_exchange_info values('{}', '{}');
            "
        , exchangeName, exchangeType);
        if let Ok(ref conn) = self.connect {
            conn.execute(sql)?
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

