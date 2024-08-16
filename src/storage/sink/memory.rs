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

    async fn exists_inner(&self, key: &str) -> Result<bool, MemoryError> {
        Ok(self.data.contains_key(key))
    }

    async fn put_bytes_inner(&mut self, value: Vec<u8>, key: String) -> Result<&Self, MemoryError> {
        self.data.insert(key, value);
        Ok(self)
    }

    async fn list_objects_inner(&self, prefix: &str) -> Result<ListKeyObjects, MemoryError> {
        let objects = self
            .data
            .iter()
            .filter(|&(key, _)| key.starts_with(prefix))
            .filter_map(|(key, _)| radix_key(prefix, key))
            .collect();

        // TODO: Limit to 1000 keys
        Ok(objects)
    }

    async fn put_object_inner<VALUE, F>(
        &mut self,
        key: String,
        value: &VALUE,
        f: F,
    ) -> Result<&Self, MemoryError>
    where
        F: Fn(&VALUE) -> Result<Vec<u8>, MemoryError>,
    {
        let serialize = f(value);

        match serialize {
            Ok(res) => self.put_bytes_inner(res, key).await,
            Err(err) => {
                let memory_error = MemoryError::from(ParserError::Serde {
                    internal: err.to_string(),
                });
                Err(memory_error)
            }
        }
    }

    async fn get_object_inner<RETURN, F>(
        &self,
        key: String,
        f: F,
    ) -> Result<Option<RETURN>, MemoryError>
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
    async fn exists<DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParser<'_, DKEY, PARSER>,
    ) -> Result<bool, Self::Error>
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        self.exists_inner(key_with_parser.key().name().as_str())
            .await
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
        self.put_object_inner(key_with_parser.key().name(), value, |value| {
            Ok(key_with_parser.parser().serialize_value(value)?)
        })
        .await
    }

    #[inline]
    async fn put_bytes<DKEY>(
        &mut self,
        value: Vec<u8>,
        key: &DKEY,
        _mime: String,
    ) -> Result<&Self, Self::Error>
    where
        DKEY: DKeyWhere,
    {
        self.put_bytes_inner(value, key.name()).await
    }

    #[inline]
    async fn get_object<RETURN, DKEY, PARSER>(
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
        .await
    }

    #[inline]
    async fn list_objects(&self, prefix: &str) -> Result<ListKeyObjects, Self::Error> {
        self.list_objects_inner(prefix).await
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
            .put_bytes(vec![], &TestKey::One, String::new())
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
            .put_bytes(vec![42, 0, 9], &TestKey::One, String::new())
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
            memory.list_objects("").await.unwrap(),
            vec![].into_iter().collect()
        );

        memory
            .put_bytes(vec![], &TestKey::One, String::new())
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects("").await.unwrap(),
            vec!["one".to_owned()].into_iter().collect(),
            "must have only `one`"
        );

        memory
            .put_bytes(vec![], &TestKey::Long, String::new())
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects("").await.unwrap(),
            vec!["one".to_owned(), "long/".to_owned()]
                .into_iter()
                .collect(),
            "`long/qux` must be split to `long/`"
        );

        memory
            .put_bytes(vec![], &TestKey::Long2, String::new())
            .await
            .unwrap();
        memory
            .put_bytes(vec![], &TestKey::VeryLong, String::new())
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects("").await.unwrap(),
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
            memory.list_objects("long").await.unwrap(),
            vec![].into_iter().collect()
        );

        memory
            .put_bytes(vec![], &TestKey::One, String::new())
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects("long").await.unwrap(),
            vec![].into_iter().collect()
        );

        assert_eq!(
            memory.list_objects("long/").await.unwrap(),
            vec![].into_iter().collect()
        );

        memory
            .put_bytes(vec![], &TestKey::Long, String::new())
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects("long").await.unwrap(),
            vec![].into_iter().collect()
        );
        assert_eq!(
            memory.list_objects("long/").await.unwrap(),
            vec!["long/qux".to_owned()].into_iter().collect()
        );

        memory
            .put_bytes(vec![], &TestKey::Long2, String::new())
            .await
            .unwrap();
        memory
            .put_bytes(vec![], &TestKey::VeryLong, String::new())
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects("long/").await.unwrap(),
            vec![
                "long/baz".to_owned(),
                "long/qux".to_owned(),
                "long/verylong/".to_owned()
            ]
            .into_iter()
            .collect()
        );

        assert_eq!(
            memory.list_objects("long/verylong/").await.unwrap(),
            vec!["long/verylong/buz".to_owned()].into_iter().collect()
        );
    }
}
