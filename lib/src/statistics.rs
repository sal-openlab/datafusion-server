// statistics.rs: Application wide system statistics information
// Sasaki, Naoki <nsasaki@sal.co.jp> July 29, 2023
//

use once_cell::sync::OnceCell;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct Statistics {
    pub server_started_at: SystemTime,
}

pub static LAZY_STATISTICS: OnceCell<Statistics> = OnceCell::new();

impl Statistics {
    pub fn new() -> Self {
        Statistics {
            server_started_at: SystemTime::now(),
        }
    }

    pub fn global() -> &'static Statistics {
        LAZY_STATISTICS
            .get()
            .expect("Statistics is not initialized")
    }
}
