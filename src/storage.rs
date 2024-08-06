pub mod key;
pub mod key_with_parser;
pub mod parser;
pub mod sink;

use core::error::Error;
use core::fmt;
use core::future::Future;
#[cfg(not(feature = "prod"))]
use std::collections::HashSet;

#[cfg(feature = "prod")]
use gxhash::HashMap;
use key::Key;
use key_with_parser::KeyWithParser;
use parser::Parser;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait KeyWhere = Key + Send + Sync;
pub trait ParserWhere = Parser + Send + Sync;
pub trait ValueWhere = Serialize + Send + Sync;
pub type ListKeyObjects = HashSet<String>;

pub trait Storage {
    type Error;

    fn exists<KEY, PARSER>(
        &self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send
    where
        KEY: KeyWhere,
        PARSER: ParserWhere;

    #[inline]
    fn put_object_if_not_exists<KEY, PARSER, VALUE>(
        &mut self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send
    where
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString + Send,
        VALUE: ValueWhere,
        Self: Send,
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

    fn put_object<VALUE, KEY, PARSER>(
        &mut self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<&Self, Self::Error>> + Send
    where
        VALUE: ValueWhere,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString + Send;

    fn put_bytes<KEY>(
        &mut self,
        value: Vec<u8>,
        key: &KEY,
        mime: String,
    ) -> impl Future<Output = Result<&Self, Self::Error>> + Send
    where
        KEY: KeyWhere;

    fn get_object<RETURN, KEY, PARSER>(
        &self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> impl Future<Output = Result<Option<RETURN>, Self::Error>> + Send
    where
        RETURN: DeserializeOwned + Send + Sync,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString;

    fn list_objects(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<ListKeyObjects, Self::Error>> + Send;
}

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

impl fmt::Display for S3Error {
    #[inline]
    #[expect(
        clippy::min_ident_chars,
        reason = "conflict with clippy::renamed_function_params lint"
    )]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for S3Error {}

#[derive(Debug)]
pub enum MemoryError {
    Serde {
        operation: String,
        key: String,
        internal: String,
    },
}

impl fmt::Display for MemoryError {
    #[inline]
    #[expect(
        clippy::min_ident_chars,
        reason = "conflict with clippy::renamed_function_params lint"
    )]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryError::Serde {
                operation,
                key,
                internal,
            } => write!(
                f,
                "Can not serde the {key} key on {operation} operation. {internal}"
            ),
        }
    }
}

impl Error for MemoryError {}
