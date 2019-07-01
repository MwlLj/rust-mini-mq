pub const success: u32 = 0;
pub const lock_error: u32 = 5;
pub const clone_error: u32 = 6;
pub const db_error: u32 = 10;
pub const send_error: u32 = 15;

pub fn error_string(err: u32) -> String {
    match err {
        success => "suceess".to_string(),
        lock_error => "lock error".to_string(),
        clone_error => "clone error".to_string(),
        db_error => "db error".to_string(),
        send_error => "send error".to_string(),
        _ => "failed".to_string()
    }
}
