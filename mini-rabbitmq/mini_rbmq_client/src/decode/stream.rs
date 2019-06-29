/*
proto:
request:
    package total size | mode ... | data
    32bit | mode:connect;vhost:test;exchangeName:test_exchange;... | package total size - type size data
*/
use std::net::TcpStream;
use std::io;
use std::io::prelude::*;

pub struct CStreamBlockParse {
    reader: Box<dyn io::Read + 'static>
}

impl CStreamBlockParse {
    pub fn line<F, T>(&mut self, startLen: u32, t: &mut T, f: &mut F) -> Result<(), &str>
        where F: FnMut(u64, Vec<u8>, &mut T) -> (bool, u32) {
        let mut len = startLen;
        let mut index = 0;
        loop {
            let mut buf = Vec::new();
            if let Ok(_) = self.reader.by_ref().take(len as u64).read_to_end(&mut buf) {
                let (b, l) = f(index, buf, t);
                if !b {
                    break;
                }
                len = l;
                index += 1;
            } else {
                return Err("read error");
            }
        }
        Ok(())
    }

    pub fn lines<F, T, L>(&mut self, startLen: u32, t: &mut T, f: &mut F, lf: L) -> Result<(), &str>
        where F: FnMut(u64, Vec<u8>, &mut T) -> (bool, u32), L: Fn(&T) -> bool {
        while let Ok(_) = self.line(startLen, t, f) {
            if !lf(t) {
                return Ok(())
            }
        }
        Ok(())
    }
}

impl CStreamBlockParse {
    pub fn new<T>(t: T) -> CStreamBlockParse
        where T: io::Read + 'static {
        CStreamBlockParse{
            reader: Box::new(t)
        }
    }
}
