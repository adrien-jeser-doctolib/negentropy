use serde::de::DeserializeOwned;

use crate::storage::copy::direct::DKeyWithParserCopy;
use crate::storage::copy::{ParserWhere, Sink, ValueWhere};
use crate::storage::sink::s3::S3;
use crate::storage::{DKeyWhere, ListKeyObjects, S3Error};

impl Sink for S3 {
    type Error = S3Error;

    #[inline]
    async fn exists_copy<DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParserCopy<'_, DKEY, PARSER>,
    ) -> Result<bool, Self::Error>
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        self.exists_inner(key_with_parser.key().name()).await
    }

    #[inline]
    async fn put_object_copy<VALUE, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParserCopy<'_, DKEY, PARSER>,
        value: &VALUE,
    ) -> Result<(), Self::Error>
    where
        VALUE: ValueWhere,
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        self.put_object_inner(
            key_with_parser.key().name(),
            key_with_parser.parser().mime(),
            value,
            |value_to_serialize| {
                Ok(key_with_parser
                    .parser()
                    .serialize_value(value_to_serialize)?)
            },
        )
        .await
    }

    #[inline]
    async fn put_bytes_copy<DKEY>(
        &mut self,
        key: &DKEY,
        mime: String,
        value: Vec<u8>,
    ) -> Result<(), Self::Error>
    where
        DKEY: DKeyWhere,
    {
        self.put_bytes_inner(key.name(), mime, value).await
    }

    #[inline]
    async fn get_object_copy<RETURN, DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParserCopy<'_, DKEY, PARSER>,
    ) -> Result<Option<RETURN>, Self::Error>
    where
        RETURN: DeserializeOwned + Send + Sync,
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        self.get_object_inner(key_with_parser.key().name(), |content| {
            Ok(key_with_parser.parser().deserialize_value(content)?)
        })
        .await
    }

    #[inline]
    async fn list_objects_copy(&self, prefix: &str) -> Result<ListKeyObjects, Self::Error> {
        self.list_objects_inner(prefix).await
    }
}
