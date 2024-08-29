use serde::de::DeserializeOwned;

use crate::storage::copy::direct::DKeyWithParserCopy;
use crate::storage::copy::{ParserWhere, Sink, ValueWhere};
use crate::storage::sink::memory::Memory;
use crate::storage::{DKeyWhere, ListKeyObjects, MemoryError};

impl Sink for Memory {
    type Error = MemoryError;

    #[inline]
    async fn exists_copy<DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParserCopy<'_, DKEY, PARSER>,
    ) -> Result<bool, Self::Error>
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        let exists = self.exists_inner(key_with_parser.key().name().as_str());
        Ok(exists)
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
        self.put_object_inner(key_with_parser.key().name(), value, |value_to_serialize| {
            let serialize_value = key_with_parser
                .parser()
                .serialize_value(value_to_serialize)?;
            Ok(serialize_value)
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
        self.put_bytes_inner(key.name(), value);
        Ok(())
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
        self.get_object_inner(&key_with_parser.key().name(), |content| {
            let deserialize_value = key_with_parser.parser().deserialize_value(content)?;
            Ok(deserialize_value)
        })
    }

    #[inline]
    async fn list_objects_copy(&self, prefix: &str) -> Result<ListKeyObjects, Self::Error> {
        Ok(self.list_objects_inner(prefix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DKey, HashSet};

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
            vec![].into_iter().collect::<HashSet<_>>()
        );

        memory
            .put_bytes_copy(&TestKey::One, String::new(), vec![])
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects_copy("").await.unwrap(),
            vec!["one".to_owned()].into_iter().collect::<HashSet<_>>(),
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
                .collect::<HashSet<_>>(),
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
                .collect::<HashSet<_>>()
        );
    }

    #[tokio::test]
    async fn list_with_prefix() {
        let mut memory = Memory::default();
        assert_eq!(memory.len(), 0);
        assert_eq!(
            memory.list_objects_copy("long").await.unwrap(),
            vec![].into_iter().collect::<HashSet<_>>()
        );

        memory
            .put_bytes_copy(&TestKey::One, String::new(), vec![])
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects_copy("long").await.unwrap(),
            vec![].into_iter().collect::<HashSet<_>>()
        );

        assert_eq!(
            memory.list_objects_copy("long/").await.unwrap(),
            vec![].into_iter().collect::<HashSet<_>>()
        );

        memory
            .put_bytes_copy(&TestKey::Long, String::new(), vec![])
            .await
            .unwrap();

        assert_eq!(
            memory.list_objects_copy("long").await.unwrap(),
            vec![].into_iter().collect::<HashSet<_>>()
        );
        assert_eq!(
            memory.list_objects_copy("long/").await.unwrap(),
            vec!["long/qux".to_owned()]
                .into_iter()
                .collect::<HashSet<_>>()
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
            .collect::<HashSet<_>>()
        );

        assert_eq!(
            memory.list_objects_copy("long/verylong/").await.unwrap(),
            vec!["long/verylong/buz".to_owned()]
                .into_iter()
                .collect::<HashSet<_>>()
        );
    }
}
