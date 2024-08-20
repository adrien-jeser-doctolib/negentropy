pub mod cache;
pub mod copy;
pub mod direct;
pub mod parser_copy;
pub mod parser_zerocopy;
pub mod sink;

use core::error::Error;
use core::fmt;

use direct::DKey;
use parser_copy::ParserCopy;
use serde::Serialize;

use crate::HashSet;

pub trait DKeyWhere = DKey + Send + Sync;
pub trait ParserWhere = ParserCopy + Send + Sync;
pub trait ValueWhere = Serialize + Send + Sync;
pub type ListKeyObjects = HashSet<String>;

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
