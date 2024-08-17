use core::num::NonZeroUsize;

use lru::LruCache;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::storage::direct::DKeyWithParser;
use crate::storage::{
    radix_key, CacheCopy, DKeyWhere, ListKeyObjects, LruError, ParserWhere, SinkCopy, ValueWhere,
};
use crate::HashSet;

pub struct Lru<STORAGE> {
    exists: HashSet<String>,
    cache: LruCache<String, Vec<u8>>,
    storage: STORAGE,
}

impl<STORAGE> Lru<STORAGE>
where
    STORAGE: SinkCopy + Send + Sync,
{
    #[inline]
    pub fn new(size: NonZeroUsize, storage: STORAGE) -> Self {
        Self {
            exists: HashSet::new(),
            cache: LruCache::new(size),
            storage,
        }
    }

    fn exists_inner(&self, key: &str) -> Result<bool, LruError> {
        Ok(self.exists.contains(key))
    }

    fn put_bytes_inner(&mut self, key: String, value: Vec<u8>) -> Result<(), LruError> {
        self.cache.put(key.clone(), value);
        self.exists.insert(key);
        Ok(())
    }

    fn get_bytes_inner(&mut self, key: &str) -> Result<Option<Vec<u8>>, LruError> {
        // TODO: Get from sink
        let bytes = self.cache.get(key).cloned();
        Ok(bytes)
    }

    fn list_objects_inner(&mut self, prefix: &str) -> Result<ListKeyObjects, LruError> {
        Ok(self
            .cache
            .iter()
            .filter(|&(key, _)| key.starts_with(prefix))
            .filter_map(|(key, _)| radix_key(prefix, key))
            .collect())
    }

    async fn get_object_cache_inner<RETURN, PARSER>(
        &mut self,
        key: String,
        f: PARSER,
    ) -> Result<Option<RETURN>, LruError>
    where
        RETURN: DeserializeOwned + Send + Sync + Serialize,
        PARSER: Fn(&[u8]) -> Result<RETURN, LruError>,
    {
        let exists = self.exists_inner(&key)?;

        if exists {
            let value = self.cache.get(&key).map(|value| f(value)).transpose()?;
            Ok(value)
        } else {
            Ok(None)
        }
    }

    async fn put_object_inner<VALUE, PARSER>(
        &mut self,
        key: String,
        value: &VALUE,
        parser: PARSER,
    ) -> Result<Vec<u8>, LruError>
    where
        PARSER: Fn(&VALUE) -> Result<Vec<u8>, LruError>,
    {
        let serialize = parser(value)?;
        self.put_bytes_inner(key, serialize.clone())?;
        Ok(serialize)
    }
}

impl<STORAGE> CacheCopy for Lru<STORAGE>
where
    STORAGE: SinkCopy + Send + Sync,
    LruError: From<<STORAGE as SinkCopy>::Error>,
{
    type Error = LruError;

    #[inline]
    async fn exists<DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParser<'_, DKEY, PARSER>,
    ) -> Result<bool, Self::Error>
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        self.exists_inner(&key_with_parser.key().name())
    }

    #[inline]
    async fn put_object<VALUE, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParser<'_, DKEY, PARSER>,
        value: &VALUE,
    ) -> Result<&Self, Self::Error>
    where
        VALUE: ValueWhere,
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        let serialize = self
            .put_object_inner(key_with_parser.key().name(), value, |value| {
                Ok(key_with_parser.parser().serialize_value(value)?)
            })
            .await?;

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
    async fn put_bytes<DKEY>(
        &mut self,
        key: &DKEY,
        mime: String,
        value: Vec<u8>,
    ) -> Result<&Self, Self::Error>
    where
        DKEY: DKeyWhere,
    {
        self.put_bytes_inner(key.name(), value.clone())?;
        self.storage.put_bytes_copy(key, mime, value).await?;
        Ok(self)
    }

    #[inline]
    async fn get_object<RETURN, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParser<'_, DKEY, PARSER>,
    ) -> Result<Option<RETURN>, Self::Error>
    where
        RETURN: Serialize + DeserializeOwned + Send + Sync,
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        let from_cache = self
            .get_object_cache_inner(key_with_parser.key().name(), |value| {
                Ok(key_with_parser.parser().deserialize_value(value)?)
            })
            .await?;

        match from_cache {
            Some(c) => Ok(Some(c)),
            None => {
                let get_object_copy = self.storage.get_object_copy(key_with_parser).await?;

                if let Some(ref value) = get_object_copy {
                    self.put_object(key_with_parser, value).await?;
                }

                Ok(get_object_copy)
            }
        }
    }

    #[inline]
    async fn list_objects(&mut self, prefix: &str) -> Result<ListKeyObjects, Self::Error> {
        self.list_objects_inner(prefix)
    }

    #[inline]
    async fn get_bytes<DKEY>(&mut self, key: &DKEY) -> Result<Option<Vec<u8>>, Self::Error>
    where
        DKEY: DKeyWhere,
    {
        self.get_bytes_inner(key.name().as_str())
    }
}
