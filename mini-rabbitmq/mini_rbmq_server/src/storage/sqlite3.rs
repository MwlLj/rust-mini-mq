extern crate sqlite3;

pub struct CSqlite3 {
    connect: sqlite3::Connection
}

impl CSqlite3 {
    pub fn connect(&mut self, vhost: &str) -> Result<(), &str> {
        let mut path = String::from(vhost);
        path.push_str(".db");
        if let Ok(conn) = sqlite3::open(path) {
            self.connect = conn;
        }
        Ok(())
    }

    pub fn new() -> CSqlite3 {
        let storage = CSqlite3{};
        storage
    }
}
