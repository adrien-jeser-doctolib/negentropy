pub mod memory;
pub mod s3;

use core::future::Future;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{Key, KeyWithParser, Parser};

pub trait KeyWhere = Key + Send + Sync;
pub trait ParserWhere = Parser + Send + Sync;
pub trait ValueWhere = Serialize + Send + Sync;
pub type ListKeyObjects = Vec<Option<String>>;

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
    ) -> impl Future<Output = Result<bool, Self::Error>>
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

    fn put_object<VALUE, KEY, PARSER>(
        &mut self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<&Self, Self::Error>>
    where
        VALUE: ValueWhere,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString;

    fn put_bytes<KEY, PARSER>(
        &mut self,
        value: Vec<u8>,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> impl Future<Output = Result<&Self, Self::Error>> + Send
    where
        KEY: KeyWhere,
        PARSER: ParserWhere;

    fn get_object<RETURN, KEY, PARSER>(
        &self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> impl Future<Output = Result<RETURN, Self::Error>>
    where
        RETURN: DeserializeOwned + Send + Sync,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString;

    fn list_objects(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<ListKeyObjects, Self::Error>>;
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

#[derive(Debug)]
pub struct MemoryError {}