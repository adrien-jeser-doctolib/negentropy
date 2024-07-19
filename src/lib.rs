#![feature(trait_alias)]
#![expect(clippy::implicit_return, reason = "return is idiomatic")]
#![expect(clippy::exhaustive_enums, reason = "break enum is acceptable")]
#![expect(
    clippy::question_mark_used,
    reason = "not use macro to attach additional information for result"
)]
#![expect(
    clippy::missing_docs_in_private_items,
    reason = "negentropy is a unstable lib"
)]
#![expect(clippy::missing_errors_doc, reason = "negentropy is a unstable api")]
#![expect(clippy::ref_patterns, reason = "ref is idiomatic")]
#![expect(
    clippy::missing_trait_methods,
    reason = "add only with lint on specific trait"
)]
#![expect(
    clippy::self_named_module_files,
    reason = "conflict with mod_module_files lint"
)]
#![expect(clippy::exhaustive_structs, reason = "Accept breaking struct")]

use serde::Serialize;
use storage::key::Key;

pub mod instance;
pub mod storage;

#[derive(Debug, Clone, Serialize)]
pub enum InstanceKey {
    Welcome,
    Initialize(String),
    Alive(String, String),
}

impl Key for InstanceKey {
    #[inline]
    fn name(&self) -> String {
        match *self {
            Self::Welcome => "instances/welcome".to_owned(),
            Self::Initialize(ref id) => format!("instances/{id}/new"),
            Self::Alive(ref id, ref timestamp) => format!("instances/{id}/alive/{timestamp}"),
        }
    }
}
