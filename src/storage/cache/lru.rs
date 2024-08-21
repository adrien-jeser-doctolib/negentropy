use core::num::NonZeroUsize;

use lru::LruCache;

use crate::storage::{radix_key, ListKeyObjects, LruError};
use crate::HashSet;

pub struct Lru<STORAGE> {
    exists: HashSet<String>,
    cache: LruCache<String, Vec<u8>>,
    pub(crate) storage: STORAGE,
}

impl<STORAGE> Lru<STORAGE>
where
    STORAGE: Send + Sync,
{
    #[inline]
    pub fn new(size: NonZeroUsize, storage: STORAGE) -> Self {
        Self {
            exists: HashSet::new(),
            cache: LruCache::new(size),
            storage,
        }
    }

    pub(crate) fn exists_inner(&self, key: &str) -> bool {
        self.exists.contains(key)
    }

    pub(crate) fn put_bytes_inner(&mut self, key: String, value: Vec<u8>) {
        self.cache.put(key.clone(), value);
        self.exists.insert(key);
    }

    pub(crate) fn get_bytes_inner(&mut self, key: &str) -> Option<Vec<u8>> {
        // TODO: Get from sink
        self.cache.get(key).cloned()
    }

    pub(crate) fn list_objects_inner(&self, prefix: &str) -> ListKeyObjects {
        self.cache
            .iter()
            .filter(|&(key, _)| key.starts_with(prefix))
            .filter_map(|(key, _)| radix_key(prefix, key))
            .collect()
    }

    pub(crate) fn get_object_cache_inner<RETURN, PARSER>(
        &mut self,
        key: &str,
        parser: PARSER,
    ) -> Result<Option<RETURN>, LruError>
    where
        RETURN: Send + Sync,
        PARSER: Fn(&[u8]) -> Result<RETURN, LruError>,
    {
        let exists = self.exists_inner(key);

        if exists {
            let value = self.cache.get(key).map(|value| parser(value)).transpose()?;
            Ok(value)
        } else {
            Ok(None)
        }
    }

    pub(crate) fn put_object_inner<VALUE, PARSER>(
        &mut self,
        key: String,
        value: &VALUE,
        parser: PARSER,
    ) -> Result<Vec<u8>, LruError>
    where
        PARSER: Fn(&VALUE) -> Result<Vec<u8>, LruError>,
    {
        let serialize = parser(value)?;
        self.put_bytes_inner(key, serialize.clone());
        Ok(serialize)
    }
}
