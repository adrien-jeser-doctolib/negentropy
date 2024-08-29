use direct::DKeyWithParserCopy;
use futures::Future;
use parser_copy::ParserCopy;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::{DKeyWhere, ListKeyObjects};

pub mod cache;
pub mod direct;
pub mod instance;
pub mod parser_copy;
pub mod sink;

pub trait ParserWhere = ParserCopy + Send + Sync;
pub trait ValueWhere = Serialize + Send + Sync;

pub trait Sink {
    type Error;

    fn exists_copy<DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParserCopy<DKEY, PARSER>,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere;

    #[inline]
    fn put_object_if_not_exists_copy<DKEY, PARSER, VALUE>(
        &mut self,
        key_with_parser: &DKeyWithParserCopy<DKEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
        VALUE: ValueWhere,
        Self: Send,
    {
        async {
            if self.exists_copy(key_with_parser).await? {
                Ok(false)
            } else {
                self.put_object_copy(key_with_parser, value).await?;
                Ok(true)
            }
        }
    }

    fn put_object_copy<VALUE, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParserCopy<DKEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        VALUE: ValueWhere,
        DKEY: DKeyWhere,
        PARSER: ParserWhere;

    fn put_bytes_copy<DKEY>(
        &mut self,
        key: &DKEY,
        mime: String,
        value: Vec<u8>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        DKEY: DKeyWhere;

    fn get_object_copy<RETURN, DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParserCopy<DKEY, PARSER>,
    ) -> impl Future<Output = Result<Option<RETURN>, Self::Error>> + Send
    where
        RETURN: DeserializeOwned + Send + Sync,
        DKEY: DKeyWhere,
        PARSER: ParserWhere;

    fn list_objects_copy(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<ListKeyObjects, Self::Error>> + Send;
}

pub trait Cache {
    type Error;

    fn exists_copy<DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParserCopy<DKEY, PARSER>,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere;

    #[inline]
    fn put_object_if_not_exists_copy<DKEY, PARSER, VALUE>(
        &mut self,
        key_with_parser: &DKeyWithParserCopy<DKEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
        VALUE: ValueWhere,
        Self: Send,
    {
        async {
            if self.exists_copy(key_with_parser).await? {
                Ok(false)
            } else {
                self.put_object_copy(key_with_parser, value).await?;
                Ok(true)
            }
        }
    }

    fn put_object_copy<VALUE, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParserCopy<DKEY, PARSER>,
        value: &VALUE,
    ) -> impl Future<Output = Result<&Self, Self::Error>> + Send
    where
        VALUE: ValueWhere,
        DKEY: DKeyWhere,
        PARSER: ParserWhere;

    fn put_bytes_copy<DKEY>(
        &mut self,
        key: &DKEY,
        mime: String,
        value: Vec<u8>,
    ) -> impl Future<Output = Result<&Self, Self::Error>> + Send
    where
        DKEY: DKeyWhere;

    fn get_object_copy<RETURN, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParserCopy<DKEY, PARSER>,
    ) -> impl Future<Output = Result<Option<RETURN>, Self::Error>> + Send
    where
        RETURN: Serialize + DeserializeOwned + Send + Sync,
        DKEY: DKeyWhere,
        PARSER: ParserWhere;

    fn get_bytes_copy<DKEY>(
        &mut self,
        key: &DKEY,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send
    where
        DKEY: DKeyWhere;

    fn list_objects_copy(
        &mut self,
        prefix: &str,
    ) -> impl Future<Output = Result<ListKeyObjects, Self::Error>> + Send;
}
