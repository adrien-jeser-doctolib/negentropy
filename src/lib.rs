#![feature(trait_alias)]
#![expect(clippy::implicit_return, reason = "return is idiomatic")]
#![expect(clippy::exhaustive_enums, reason = "break enum is acceptable")]
#![expect(
    clippy::question_mark_used,
    reason = "not use macro to attach additional information for result"
)]
#![expect(clippy::missing_docs_in_private_items, reason = "unstable lib")]
#![expect(clippy::missing_errors_doc, reason = "unstable api")]
#![expect(clippy::ref_patterns, reason = "ref is idiomatic")]
#![expect(clippy::missing_trait_methods, reason = "add only specific trait")]

use core::option::Option;
use serde::Serialize;
use storage::ValueWhere;

pub mod live;
pub mod parser;
pub mod s3;
pub mod storage;

#[derive(Debug)]
pub enum S3Error {
    Serde {
        operation: String,
        key: String,
        internal: String,
    },
    S3Bucket {
        operation: String,
        bucket: String,
        internal: String,
    },
    S3Object {
        operation: String,
        key: String,
        internal: String,
    },
    S3List {
        operation: String,
        prefix: String,
        internal: Option<String>,
    },
    S3Exists {
        operation: String,
        key: String,
        internal: String,
    },
    S3ListHandle,
    NotExistsObject(String),
    EnvConfig(String),
}

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
    pub fn new(key: KEY, parser: PARSER) -> Self {
        Self { key, parser }
    }

    pub fn key(&self) -> &KEY {
        &self.key
    }

    pub fn parser(&self) -> &PARSER {
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
