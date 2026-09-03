//! The in-process write barrier a topology change arms while it
//! rewires where a database's rows live, and the coordination seam a
//! cutover drives peer pgdog instances through.

pub mod barrier;

pub(crate) mod coordination;

pub(crate) use coordination::{Coordinator, Discovery};
