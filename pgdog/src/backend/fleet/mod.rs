//! Cross-instance coordination for a fleet of pgdog processes sharing
//! the same databases: who is alive (registry), and the local write
//! barrier a coordinated operation arms.

pub mod barrier;
#[allow(dead_code)] // TODO: remove once the coordination consumers land
pub mod protocol;
pub mod registry;
