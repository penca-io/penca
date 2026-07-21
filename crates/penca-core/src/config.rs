//! Shared config helpers for reading required environment variables.
//!
//! These utilities are used by every microservice binary. Each binary still
//! owns its own config struct — this module only provides the low-level
//! helpers for reading and parsing env vars.

/// Read a required environment variable, panicking with a clear message if absent.
pub fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set"))
}

/// Read and parse a required environment variable, panicking with a clear message on failure.
pub fn required_env_parsed<T: std::str::FromStr>(name: &str) -> T
where
    T::Err: std::fmt::Display,
{
    let val = required_env(name);
    val.parse()
        .unwrap_or_else(|e| panic!("{name}={val:?} is not valid: {e}"))
}
