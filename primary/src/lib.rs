// In primary/src/lib.rs

// Copyright(C) Facebook, Inc. and its affiliates.
#[macro_use]
mod error;
mod aggregators;
mod certificate_waiter;
pub mod core; // SỬA LỖI: Chuyển 'mod core;' thành 'pub mod core;'
mod garbage_collector;
mod header_waiter;
mod helper;
mod messages;
mod primary;
mod proposer;
mod synchronizer;

#[cfg(test)]
#[path = "tests/common.rs"]
mod common;

pub use crate::core::Core;
pub use crate::messages::{Certificate, Header};
pub use crate::primary::{
    Primary, PrimaryWorkerMessage, ReconfigureNotification, WorkerPrimaryMessage,
};

// ĐỊNH NGHĨA: Thêm các type alias cho rõ ràng
pub type Round = u64;
pub type Epoch = u64;
