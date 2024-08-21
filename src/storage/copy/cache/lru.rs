use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::storage::cache::lru::Lru;
use crate::storage::copy::direct::DKeyWithParserCopy;
use crate::storage::copy::{CacheCopy, ParserWhere, SinkCopy, ValueWhere};
use crate::storage::{DKeyWhere, ListKeyObjects, LruError};

impl<STORAGE> CacheCopy for Lru<STORAGE>
where
    STORAGE: SinkCopy + Send + Sync,
    LruError: From<<STORAGE as SinkCopy>::Error>,
{
    type Error = LruError;

    #[inline]
    async fn exists_copy<DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParserCopy<'_, DKEY, PARSER>,
    ) -> Result<bool, Self::Error>
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        Ok(self.exists_inner(&key_with_parser.key().name()))
    }

    #[inline]
    async fn put_object_copy<VALUE, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParserCopy<'_, DKEY, PARSER>,
        value: &VALUE,
    ) -> Result<&Self, Self::Error>
    where
        VALUE: ValueWhere,
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        let serialize =
            self.put_object_inner(key_with_parser.key().name(), value, |value_to_serialize| {
                Ok(key_with_parser
                    .parser()
                    .serialize_value(value_to_serialize)?)
            })?;

        self.storage
            .put_bytes_copy(
                key_with_parser.key(),
                key_with_parser.parser().mime(),
                serialize,
            )
            .await?;

        Ok(self)
    }

    #[inline]
    async fn put_bytes_copy<DKEY>(
        &mut self,
        key: &DKEY,
        mime: String,
        value: Vec<u8>,
    ) -> Result<&Self, Self::Error>
    where
        DKEY: DKeyWhere,
    {
        self.put_bytes_inner(key.name(), value.clone());
        self.storage.put_bytes_copy(key, mime, value).await?;
        Ok(self)
    }

    #[inline]
    async fn get_object_copy<RETURN, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParserCopy<'_, DKEY, PARSER>,
    ) -> Result<Option<RETURN>, Self::Error>
    where
        RETURN: Serialize + DeserializeOwned + Send + Sync,
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        let from_cache = self.get_object_cache_inner(&key_with_parser.key().name(), |value| {
            Ok(key_with_parser.parser().deserialize_value(value)?)
        })?;

        if let Some(value_from_cache) = from_cache {
            Ok(Some(value_from_cache))
        } else {
            let get_object_copy = self.storage.get_object_copy(key_with_parser).await?;

            if let Some(ref value) = get_object_copy {
                self.put_object_copy(key_with_parser, value).await?;
            }

            Ok(get_object_copy)
        }
    }

    #[inline]
    async fn list_objects_copy(&mut self, prefix: &str) -> Result<ListKeyObjects, Self::Error> {
        Ok(self.list_objects_inner(prefix))
    }

    #[inline]
    async fn get_bytes_copy<DKEY>(&mut self, key: &DKEY) -> Result<Option<Vec<u8>>, Self::Error>
    where
        DKEY: DKeyWhere,
    {
        Ok(self.get_bytes_inner(key.name().as_str()))
    }
}
