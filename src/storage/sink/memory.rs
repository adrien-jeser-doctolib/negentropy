use serde::de::DeserializeOwned;

use crate::storage::direct::DKeyWithParser;
use crate::storage::{
    radix_key, DKeyWhere, ListKeyObjects, MemoryError, ParserError, ParserWhere, SinkCopy,
    ValueWhere,
};
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

    fn exists_inner(&self, key: &str) -> Result<bool, MemoryError> {
        Ok(self.data.contains_key(key))
    }

    fn put_bytes_inner(&mut self, key: String, value: Vec<u8>) -> Result<(), MemoryError> {
        self.data.insert(key, value);
        Ok(())
    }

    fn list_objects_inner(&self, prefix: &str) -> Result<ListKeyObjects, MemoryError> {
        let objects = self
            .data
            .iter()
            .filter(|&(key, _)| key.starts_with(prefix))
            .filter_map(|(key, _)| radix_key(prefix, key))
            .collect();

        // TODO: Limit to 1000 keys
        Ok(objects)
    }

    fn put_object_inner<VALUE, F>(
        &mut self,
        key: String,
        value: &VALUE,
        f: F,
    ) -> Result<(), MemoryError>
    where
        F: Fn(&VALUE) -> Result<Vec<u8>, MemoryError>,
    {
        let serialize = f(value);

        match serialize {
            Ok(res) => self.put_bytes_inner(key, res),
            Err(err) => {
                let memory_error = MemoryError::from(ParserError::Serde {
                    internal: err.to_string(),
                });
                Err(memory_error)
            }
        }
    }

    fn get_object_inner<RETURN, F>(&self, key: String, f: F) -> Result<Option<RETURN>, MemoryError>
    where
        RETURN: Send + Sync,
        F: Fn(&[u8]) -> Result<RETURN, MemoryError>,
    {
        let object = self.data.get(&key);
        let value = object.map_or_else(
            || Ok(None),
            |content| f(content).map(|content| Some(content)),
        )?;

        Ok(value)
    }
}

impl SinkCopy for Memory {
    type Error = MemoryError;

    #[inline]
    async fn exists_copy<DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParser<'_, DKEY, PARSER>,
    ) -> Result<bool, Self::Error>
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        self.exists_inner(key_with_parser.key().name().as_str())
    }

    #[inline]
    async fn put_object_copy<VALUE, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParser<'_, DKEY, PARSER>,
        value: &VALUE,
    ) -> Result<(), Self::Error>
    where
        VALUE: ValueWhere,
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        self.put_object_inner(key_with_parser.key().name(), value, |value| {
            Ok(key_with_parser.parser().serialize_value(value)?)
        })
    }

    #[inline]
    async fn put_bytes_copy<DKEY>(
        &mut self,
        key: &DKEY,
        _mime: String,
        value: Vec<u8>,
    ) -> Result<(), Self::Error>
    where
        DKEY: DKeyWhere,
    {
        self.put_bytes_inner(key.name(), value)
    }

    #[inline]
    async fn get_object_copy<RETURN, DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParser<'_, DKEY, PARSER>,
    ) -> Result<Option<RETURN>, Self::Error>
    where
        RETURN: DeserializeOwned + Send + Sync,
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        self.get_object_inner(key_with_parser.key().name(), |content| {
            Ok(key_with_parser.parser().deserialize_value(content)?)
        })
    }

    #[inline]
    async fn list_objects_copy(&self, prefix: &str) -> Result<ListKeyObjects, Self::Error> {
        self.list_objects_inner(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DKey;

    enum TestKey {
        One,
        Long,
        Long2,
        VeryLong,
    }

    impl DKey for TestKey {
        fn name(&self) -> String {
            match *self {
                Self::One => "one".to_owned(),
                Self::Long => "long/qux".to_owned(),
                Self::Long2 => "long/baz".to_owned(),
                Self::VeryLong => "long/verylong/buz".to_owned(),
            }
        }
    }

    #[test]
    fn empty() {
        let memory = Memory::default();
        assert_eq!(memory.len(), 0);
    }

    #[tokio::test]
    async fn empty_bytes() {
        let mut memory = Memory::default();
        assert_eq!(memory.len(), 0);
        memory
            .put_bytes_copy(&TestKey::One, String::new(), vec![])
            .await
            .unwrap();
        assert_eq!(memory.len(), 1);
        assert_eq!(memory.get_bytes(&TestKey::One).unwrap(), &Vec::<u8>::new());
    }

    #[tokio::test]
    async fn bytes() {
        let mut memory = Memory::default();
        assert_eq!(memory.len(), 0);
        memory
            .put_bytes_copy(&TestKey::One, String::new(), vec![42, 0, 9])
            .await
            .unwrap();
        assert_eq!(memory.len(), 1);
        assert_eq!(memory.get_bytes(&TestKey::One).unwrap(), &vec![42, 0, 9]);
    }

    #[tokio::test]
    async fn list_root() {
        let mut memory = Memory::default();
        assert_eq!(memory.len(), 0);
        assert_eq!(
            memory.list_objects_copy("").await.unwrap(),
            vec![].into_iter().collect()
        );

        memory
            .put_bytes_copy(&TestKey::One, String::new(), vec![])
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects_copy("").await.unwrap(),
            vec!["one".to_owned()].into_iter().collect(),
            "must have only `one`"
        );

        memory
            .put_bytes_copy(&TestKey::Long, String::new(), vec![])
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects_copy("").await.unwrap(),
            vec!["one".to_owned(), "long/".to_owned()]
                .into_iter()
                .collect(),
            "`long/qux` must be split to `long/`"
        );

        memory
            .put_bytes_copy(&TestKey::Long2, String::new(), vec![])
            .await
            .unwrap();
        memory
            .put_bytes_copy(&TestKey::VeryLong, String::new(), vec![])
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects_copy("").await.unwrap(),
            vec!["one".to_owned(), "long/".to_owned()]
                .into_iter()
                .collect()
        );
    }

    #[tokio::test]
    async fn list_with_prefix() {
        let mut memory = Memory::default();
        assert_eq!(memory.len(), 0);
        assert_eq!(
            memory.list_objects_copy("long").await.unwrap(),
            vec![].into_iter().collect()
        );

        memory
            .put_bytes_copy(&TestKey::One, String::new(), vec![])
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects_copy("long").await.unwrap(),
            vec![].into_iter().collect()
        );

        assert_eq!(
            memory.list_objects_copy("long/").await.unwrap(),
            vec![].into_iter().collect()
        );

        memory
            .put_bytes_copy(&TestKey::Long, String::new(), vec![])
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects_copy("long").await.unwrap(),
            vec![].into_iter().collect()
        );
        assert_eq!(
            memory.list_objects_copy("long/").await.unwrap(),
            vec!["long/qux".to_owned()].into_iter().collect()
        );

        memory
            .put_bytes_copy(&TestKey::Long2, String::new(), vec![])
            .await
            .unwrap();
        memory
            .put_bytes_copy(&TestKey::VeryLong, String::new(), vec![])
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects_copy("long/").await.unwrap(),
            vec![
                "long/baz".to_owned(),
                "long/qux".to_owned(),
                "long/verylong/".to_owned()
            ]
            .into_iter()
            .collect()
        );

        assert_eq!(
            memory.list_objects_copy("long/verylong/").await.unwrap(),
            vec!["long/verylong/buz".to_owned()].into_iter().collect()
        );
    }
}
