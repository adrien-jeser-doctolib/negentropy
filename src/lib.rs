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
use storage::ValueWhere;

pub mod live;
pub mod parser;
pub mod storage;

pub trait Key {
    fn name(&self) -> String;
}

pub trait Parser {
    type Error;

    fn serialize_value<VALUE>(&self, value: &VALUE) -> Result<Vec<u8>, Self::Error>
    where
        VALUE: ValueWhere;

    fn deserialize_value<CONTENT>(&self, content: &[u8]) -> Result<CONTENT, Self::Error>
    where
        CONTENT: for<'content> serde::Deserialize<'content>;

    fn mime(&self) -> String;
}

pub struct KeyWithParser<KEY, PARSER>
where
    KEY: Key,
    PARSER: Parser,
{
    key: KEY,
    parser: PARSER,
}

impl<KEY, PARSER> KeyWithParser<KEY, PARSER>
where
    KEY: Key,
    PARSER: Parser,
{
    #[inline]
    pub const fn new(key: KEY, parser: PARSER) -> Self {
        Self { key, parser }
    }

    #[inline]
    pub const fn key(&self) -> &KEY {
        &self.key
    }

    #[inline]
    pub const fn parser(&self) -> &PARSER {
        &self.parser
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum LiveKey {
    Welcome,
    Alive(String),
}

impl Key for LiveKey {
    #[inline]
    fn name(&self) -> String {
        match *self {
            Self::Welcome => "live/welcome".to_owned(),
            Self::Alive(ref id) => format!("live/alive-{id}"),
        }
    }
}
