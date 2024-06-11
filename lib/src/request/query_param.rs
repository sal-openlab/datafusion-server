// query_param.rs - URL query parameters handler
// Sasaki, Naoki <nsasaki@sal.co.jp> October 16, 2022
//

// TODO: to be refactored, use generics or other **smart** implementation

#[allow(dead_code)]
pub fn u32_or_default(value: Option<&String>, default: u32) -> u32 {
    match value {
        Some(v) => v.parse::<u32>().unwrap_or(default),
        None => default,
    }
}

#[allow(dead_code)]
pub fn usize_or_default(value: Option<&String>, default: usize) -> usize {
    match value {
        Some(v) => v.parse::<usize>().unwrap_or(default),
        None => default,
    }
}

#[allow(dead_code)]
pub fn str_or_default(value: Option<&String>, default: &str) -> String {
    match value {
        Some(v) => v.to_string(),
        None => default.to_string(),
    }
}

#[allow(dead_code)]
pub fn bool_or_default(value: Option<&String>, default: bool) -> bool {
    match value {
        Some(v) => matches!(v.to_ascii_lowercase().as_str(), "true" | "yes" | "on" | "1"),
        None => default,
    }
}
