extern crate mini_rbmq_server;

use mini_rbmq_server::net::tcp;
use rust_parse::cmd::CCmd;

const argServerHost: &str = "-host";
const argServerPort: &str = "-port";
const argStorageRoot: &str = "-storage-root";
const argThreadMax: &str = "-thread-max";

fn printHelp() {
    let mut message = String::new();
    message.push_str("options: \n");
    message.push_str("\t-host: server listen ip, exp: 0.0.0.0\n");
    message.push_str("\t-port: server listen port, exp: 60000\n");
    message.push_str("\t-storage-root: storage root, exp: mini_rbmq");
    message.push_str("\t-thread-max: thread max number, exp: 10\n");
    print!("{}", message);
}

fn main() {
    printHelp();
    
    let mut cmdHandler = CCmd::new();
    let host = cmdHandler.register(argServerHost, "0.0.0.0");
    let port = cmdHandler.register(argServerPort, "60000");
    let storageRoot = cmdHandler.register(argStorageRoot, "mini_rbmq");
    let threadMax = cmdHandler.register(argThreadMax, "10");
    cmdHandler.parse();

    let host = host.borrow();
    let port = port.borrow();
    let threadMax = threadMax.borrow();
    let storageRoot = storageRoot.borrow();

    let mut server = String::new();
    server.push_str(&(*host));
    server.push_str(":");
    server.push_str(&(*port));

    if let Ok(threadMax) = threadMax.trim().parse() {
        let conn = tcp::CTcp::new(threadMax);
        conn.start(&server, storageRoot.to_string());
    } else {
        println!("threadMax must be number!!!");
    }
}
