use std::num::NonZeroUsize;

use lru::LruCache;
use serde::de::DeserializeOwned;

use crate::storage::key_with_parser::KeyWithParser;
use crate::storage::{KeyWhere, ListKeyObjects, MemoryError, Middleware, ParserWhere, ValueWhere};
use crate::HashSet;

pub struct Lru<MIDDLEWARE> {
    exists: HashSet<String>,
    cache: LruCache<String, Vec<u8>>,
    storage: MIDDLEWARE,
}

impl<MIDDLEWARE> Lru<MIDDLEWARE>
where
    MIDDLEWARE: Middleware + Send + Sync,
{
    pub fn new(size: NonZeroUsize, storage: MIDDLEWARE) -> Self {
        Self {
            exists: HashSet::new(),
            cache: LruCache::new(size),
            storage,
        }
    }
}

impl<MIDDLEWARE> Middleware for Lru<MIDDLEWARE>
where
    MIDDLEWARE: Middleware + Send + Sync,
    MemoryError: From<<MIDDLEWARE as Middleware>::Error>,
{
    type Error = MemoryError;

    async fn exists<KEY, PARSER>(
        &self,
        key_with_parser: &KeyWithParser<'_, KEY, PARSER>,
    ) -> Result<bool, Self::Error>
    where
        KEY: KeyWhere,
        PARSER: ParserWhere,
    {
        Ok(self.exists.contains(&key_with_parser.key().name()))
    }

    async fn put_object<VALUE, KEY, PARSER>(
        &mut self,
        key_with_parser: &KeyWithParser<'_, KEY, PARSER>,
        value: &VALUE,
    ) -> Result<&Self, Self::Error>
    where
        VALUE: ValueWhere,
        KEY: KeyWhere,
        PARSER: ParserWhere,
    {
        // self.storage
        //     .put_object(value.clone(), key_with_parser)
        //     .await;
        // self.cache.put(key_with_parser.key().name(), value);
        self.exists.insert(key_with_parser.key().name());
        Ok(self)
    }

    async fn put_bytes<KEY>(
        &mut self,
        value: Vec<u8>,
        key: &KEY,
        mime: String,
    ) -> Result<&Self, Self::Error>
    where
        KEY: KeyWhere,
    {
        self.storage.put_bytes(value.clone(), key, mime).await?;
        self.cache.put(key.name(), value);
        self.exists.insert(key.name());
        Ok(self)
    }

    async fn get_object<RETURN, KEY, PARSER>(
        &mut self,
        key_with_parser: &KeyWithParser<'_, KEY, PARSER>,
    ) -> Result<Option<RETURN>, Self::Error>
    where
        RETURN: DeserializeOwned + Send + Sync,
        KEY: KeyWhere,
        PARSER: ParserWhere,
    {
        let value = self
            .cache
            .get(&key_with_parser.key().name())
            .map(|value| key_with_parser.parser().deserialize_value(value))
            .transpose()?;
        Ok(value)
    }

    async fn list_objects(&mut self, prefix: &str) -> Result<ListKeyObjects, Self::Error> {
        todo!()
    }
}
