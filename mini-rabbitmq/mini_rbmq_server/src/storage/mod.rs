pub mod sqlite3;

pub trait IStorage {
    fn connect(&self, vhost: &str) -> Result<(), &str>;
}
