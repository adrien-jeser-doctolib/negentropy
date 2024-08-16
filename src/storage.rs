pub mod cache;
pub mod direct;
pub mod parser;
pub mod sink;

use core::error::Error;
use core::fmt;
use core::future::Future;

use direct::{DKey, DKeyWithParser};
use parser::Parser;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::HashSet;

pub trait DKeyWhere = DKey + Send + Sync;
pub trait ParserWhere = Parser + Send + Sync;
pub trait ValueWhere = Serialize + Send + Sync;
pub type ListKeyObjects = HashSet<String>;

pub trait Sink {
    type Error;

    fn exists<DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParser<DKEY, PARSER>,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere;

    #[inline]
    fn put_object_if_not_exists<DKEY, PARSER, VALUE>(
        &mut self,
        key_with_parser: &DKeyWithParser<DKEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send
    where
        DKEY: DKeyWhere,
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

    fn put_object<VALUE, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParser<DKEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<&Self, Self::Error>> + Send
    where
        VALUE: ValueWhere,
        DKEY: DKeyWhere,
        PARSER: ParserWhere;

    fn put_bytes<DKEY>(
        &mut self,
        value: Vec<u8>,
        key: &DKEY,
        mime: String,
    ) -> impl Future<Output = Result<&Self, Self::Error>> + Send
    where
        DKEY: DKeyWhere;

    fn get_object<RETURN, DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParser<DKEY, PARSER>,
    ) -> impl Future<Output = Result<Option<RETURN>, Self::Error>> + Send
    where
        RETURN: DeserializeOwned + Send + Sync,
        DKEY: DKeyWhere,
        PARSER: ParserWhere;

    fn list_objects(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<ListKeyObjects, Self::Error>> + Send;
}

pub trait Cache {
    type Error;

    fn exists<DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParser<DKEY, PARSER>,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere;

    #[inline]
    fn put_object_if_not_exists<DKEY, PARSER, VALUE>(
        &mut self,
        key_with_parser: &DKeyWithParser<DKEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send
    where
        DKEY: DKeyWhere,
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

    fn put_object<VALUE, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParser<DKEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<&Self, Self::Error>> + Send
    where
        VALUE: ValueWhere,
        DKEY: DKeyWhere,
        PARSER: ParserWhere;

    fn put_bytes<DKEY>(
        &mut self,
        value: Vec<u8>,
        key: &DKEY,
        mime: String,
    ) -> impl Future<Output = Result<&Self, Self::Error>> + Send
    where
        DKEY: DKeyWhere;

    fn get_object<RETURN, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParser<DKEY, PARSER>,
    ) -> impl Future<Output = Result<Option<RETURN>, Self::Error>> + Send
    where
        RETURN: DeserializeOwned + Send + Sync + Serialize,
        DKEY: DKeyWhere,
        PARSER: ParserWhere;

    fn get_bytes<DKEY>(
        &mut self,
        key: &DKEY,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send
    where
        DKEY: DKeyWhere;

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
    #[inline]
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
        match *self {
            Self::Serde(ref err) => write!(f, "ParseMemory: {err}"),
        }
    }
}

impl From<ParserError> for MemoryError {
    #[inline]
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
        match *self {
            Self::Serde { ref internal } => write!(f, "Can not serde : {internal}"),
        }
    }
}

impl Error for ParserError {}

#[derive(Debug)]
pub enum LruError {
    S3(S3Error),
    Memory(MemoryError),
    Parser(ParserError),
}

impl fmt::Display for LruError {
    #[inline]
    #[expect(
        clippy::min_ident_chars,
        reason = "conflict with clippy::renamed_function_params lint"
    )]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::S3(ref err) => write!(f, "LruError: {err}"),
            Self::Parser(ref err) => write!(f, "ParserError: {err}"),
            Self::Memory(ref err) => write!(f, "MemoryError: {err}"),
        }
    }
}

impl From<MemoryError> for LruError {
    #[inline]
    fn from(value: MemoryError) -> Self {
        Self::Memory(value)
    }
}

impl From<S3Error> for LruError {
    #[inline]
    fn from(value: S3Error) -> Self {
        Self::S3(value)
    }
}

impl From<ParserError> for LruError {
    #[inline]
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
