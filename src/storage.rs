pub mod cache;
pub mod key;
pub mod key_with_parser;
pub mod parser;
pub mod sink;

use core::error::Error;
use core::fmt;
use core::future::Future;

use key::Key;
use key_with_parser::KeyWithParser;
use parser::Parser;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::HashSet;

pub trait KeyWhere = Key + Send + Sync;
pub trait ParserWhere = Parser + Send + Sync;
pub trait ValueWhere = Serialize + Send + Sync;
pub type ListKeyObjects = HashSet<String>;

pub trait Sink {
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
        PARSER: ParserWhere;

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
        PARSER: ParserWhere;

    fn list_objects(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<ListKeyObjects, Self::Error>> + Send;
}

pub trait Cache {
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
        PARSER: ParserWhere;

    fn put_bytes<KEY>(
        &mut self,
        value: Vec<u8>,
        key: &KEY,
        mime: String,
    ) -> impl Future<Output = Result<&Self, Self::Error>> + Send
    where
        KEY: KeyWhere;

    fn get_object<RETURN, KEY, PARSER>(
        &mut self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> impl Future<Output = Result<Option<RETURN>, Self::Error>> + Send
    where
        RETURN: DeserializeOwned + Send + Sync + Serialize,
        KEY: KeyWhere,
        PARSER: ParserWhere;

    fn list_objects(
        &mut self,
        prefix: &str,
    ) -> impl Future<Output = Result<ListKeyObjects, Self::Error>> + Send;
}

#[derive(Debug)]
pub enum S3Error {
    Serde(ParserError),
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

impl From<ParserError> for S3Error {
    fn from(value: ParserError) -> Self {
        Self::Serde(value)
    }
}

#[derive(Debug)]
pub enum MemoryError {
    Serde(ParserError),
}

impl fmt::Display for MemoryError {
    #[inline]
    #[expect(
        clippy::min_ident_chars,
        reason = "conflict with clippy::renamed_function_params lint"
    )]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemoryError::Serde(err) => write!(f, "ParseMemory: {err}"),
        }
    }
}

impl From<ParserError> for MemoryError {
    fn from(value: ParserError) -> Self {
        Self::Serde(value)
    }
}

impl Error for MemoryError {}

#[derive(Debug)]
pub enum ParserError {
    Serde { internal: String },
}

impl fmt::Display for ParserError {
    #[inline]
    #[expect(
        clippy::min_ident_chars,
        reason = "conflict with clippy::renamed_function_params lint"
    )]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serde { internal } => write!(f, "Can not serde : {internal}"),
        }
    }
}

impl Error for ParserError {}

#[derive(Debug)]
pub enum LruError {
    S3(S3Error),
    Parser(ParserError),
}

impl fmt::Display for LruError {
    #[inline]
    #[expect(
        clippy::min_ident_chars,
        reason = "conflict with clippy::renamed_function_params lint"
    )]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LruError::S3(err) => write!(f, "LruError: {err}"),
            LruError::Parser(err) => write!(f, "ParserError: {err}"),
        }
    }
}

impl From<S3Error> for LruError {
    fn from(value: S3Error) -> Self {
        Self::S3(value)
    }
}

impl From<ParserError> for LruError {
    fn from(value: ParserError) -> Self {
        Self::Parser(value)
    }
}

fn radix_key(prefix: &str, key: &String) -> Option<String> {
    let delimiter = '/';
    let prefix_len = prefix.len();
    let (_, radical) = key.split_at(prefix_len);
    let radical_key = radical.split_once(delimiter);

    match radical_key {
        None => Some(key.to_owned()),
        Some((radical_without_suffix, _)) => {
            if radical_without_suffix.is_empty() {
                None
            } else {
                Some(format!("{prefix}{radical_without_suffix}{delimiter}"))
            }
        }
    }
}
