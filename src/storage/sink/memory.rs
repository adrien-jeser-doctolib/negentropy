use crate::storage::{radix_key, DKeyWhere, ListKeyObjects, MemoryError, ParserError};
use crate::HashMap;

#[derive(Default)]
pub struct Memory {
    data: HashMap<String, Vec<u8>>,
}

impl Memory {
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn get_bytes<DKEY>(&mut self, key: &DKEY) -> Option<&Vec<u8>>
    where
        DKEY: DKeyWhere,
    {
        self.data.get(&key.name())
    }

    pub(crate) fn exists_inner(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    pub(crate) fn put_bytes_inner(&mut self, key: String, value: Vec<u8>) {
        self.data.insert(key, value);
    }

    pub(crate) fn list_objects_inner(&self, prefix: &str) -> ListKeyObjects {
        // TODO: Limit to 1000 keys
        self.data
            .iter()
            .filter(|&(key, _)| key.starts_with(prefix))
            .filter_map(|(key, _)| radix_key(prefix, key))
            .collect()
    }

    pub(crate) fn put_object_inner<VALUE, PARSER>(
        &mut self,
        key: String,
        value: &VALUE,
        parser: PARSER,
    ) -> Result<(), MemoryError>
    where
        PARSER: Fn(&VALUE) -> Result<Vec<u8>, MemoryError>,
    {
        let serialize = parser(value);

        match serialize {
            Ok(res) => {
                self.put_bytes_inner(key, res);
                Ok(())
            }
            Err(err) => {
                let memory_error = MemoryError::from(ParserError::Serde {
                    internal: err.to_string(),
                });
                Err(memory_error)
            }
        }
    }

    pub(crate) fn get_object_inner<RETURN, PARSER>(
        &self,
        key: &str,
        parser: PARSER,
    ) -> Result<Option<RETURN>, MemoryError>
    where
        RETURN: Send + Sync,
        PARSER: Fn(&[u8]) -> Result<RETURN, MemoryError>,
    {
        let object = self.data.get(key);
        let value = object.map_or_else(
            || Ok(None),
            |content_to_deserialize| parser(content_to_deserialize).map(|content| Some(content)),
        )?;

        Ok(value)
    }
}
