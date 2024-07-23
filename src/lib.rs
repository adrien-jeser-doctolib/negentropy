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

use core::{future::Future, option::Option};
use serde::{de::DeserializeOwned, Serialize};

pub mod live;
pub mod parser;
pub mod s3;

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

pub trait KeyWhere = Key + Send + Sync;
pub trait ParserWhere = Parser + Send + Sync;
pub trait ValueWhere = Serialize + Send + Sync;

pub trait Storage {
    fn exists<KEY, PARSER>(
        &self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> impl Future<Output = Result<bool, S3Error>>
    where
        KEY: KeyWhere,
        PARSER: ParserWhere;

    #[inline]
    fn put_object_if_not_exists<KEY, PARSER, VALUE>(
        &self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<bool, S3Error>>
    where
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString,
        VALUE: ValueWhere,
        Self: Sync,
    {
        async {
            if self.exists(key_with_parser).await? {
                Ok(false)
            } else {
                self.put_object(key_with_parser, value).await?;
                Ok(true)
            }
        }
    }

    #[inline]
    fn put_object<VALUE, KEY, PARSER>(
        &self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<&Self, S3Error>>
    where
        VALUE: ValueWhere,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString,
    {
        async {
            let serialize = key_with_parser.parser().serialize_value(value);

            match serialize {
                Ok(res) => self.put_bytes(res, key_with_parser).await,
                Err(err) => Err(S3Error::S3Object {
                    operation: "put_object".to_owned(),
                    key: key_with_parser.key().name(),
                    internal: err.to_string(),
                }),
            }
        }
    }

    fn put_bytes<KEY, PARSER>(
        &self,
        value: Vec<u8>,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> impl Future<Output = Result<&Self, S3Error>> + Send
    where
        KEY: KeyWhere,
        PARSER: ParserWhere;

    fn get_object<RETURN, KEY, PARSER>(
        &self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> impl Future<Output = Result<RETURN, S3Error>>
    where
        RETURN: DeserializeOwned + Send + Sync,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString;

    fn list_objects(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<Vec<Option<String>>, S3Error>>;
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
