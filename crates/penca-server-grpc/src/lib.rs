//! gRPC server implementations for Penca proto services.
//!
//! Each proto service runs as an independent microservice. This crate
//! provides the tonic service trait implementations and binary entrypoints.

pub mod config;
mod ipc;
pub mod lifecycle;
pub mod query;
pub mod server;
mod status;
mod validation;
pub mod write;
