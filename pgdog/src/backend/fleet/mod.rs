//! Cross-instance coordination for a fleet of pgdog processes sharing
//! the same databases: who is alive (registry), a coordinator/follower
//! protocol over a designated "medium" database, and the local write
//! barrier a coordination arms.

pub mod barrier;
pub mod registry;

// TODO: remove the dead_code/unused_imports allows once the
// coordination consumers land.
#[allow(dead_code)]
pub(crate) mod coordinator;
#[allow(dead_code)]
pub(crate) mod follower;
#[allow(dead_code)]
pub(crate) mod protocol;

#[allow(unused_imports)]
pub(crate) use coordinator::{Coordinator, Discovery};
#[allow(unused_imports)]
pub(crate) use follower::Follower;
#[allow(unused_imports)]
pub(crate) use protocol::Topic;
