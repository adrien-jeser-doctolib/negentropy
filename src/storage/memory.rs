use gxhash::HashMap;
use serde::de::DeserializeOwned;

use super::{KeyWhere, ListKeyObjects, MemoryError, ParserWhere, Storage};
use crate::key_with_parser::KeyWithParser;
use crate::parser::Parser;

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
    pub fn get_bytes<KEY>(&mut self, key: &KEY) -> Result<&Vec<u8>, MemoryError>
    where
        KEY: KeyWhere,
    {
        let object = self.data.get(&key.name());
        object.ok_or_else(|| MemoryError::NotExistsObject(key.name()))
    }
}

impl Storage for Memory {
    type Error = MemoryError;

    #[inline]
    async fn exists<KEY, PARSER>(
        &self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> Result<bool, Self::Error>
    where
        KEY: KeyWhere,
        PARSER: ParserWhere,
    {
        Ok(self.data.contains_key(&key_with_parser.key().name()))
    }

    #[inline]
    async fn put_object<VALUE, KEY, PARSER>(
        &mut self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
        value: &VALUE,
    ) -> Result<&Self, Self::Error>
    where
        VALUE: super::ValueWhere,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString + Send,
    {
        let serialize = key_with_parser.parser().serialize_value(value);

        match serialize {
            Ok(res) => {
                self.put_bytes(res, key_with_parser.key(), key_with_parser.parser().mime())
                    .await
            }
            Err(err) => Err(MemoryError::Serde {
                operation: "put_object".to_owned(),
                key: key_with_parser.key().name(),
                internal: err.to_string(),
            }),
        }
    }

    #[inline]
    async fn put_bytes<KEY>(
        &mut self,
        value: Vec<u8>,
        key: &KEY,
        _mime: String,
    ) -> Result<&Self, Self::Error>
    where
        KEY: KeyWhere,
    {
        self.data.insert(key.name(), value);
        Ok(self)
    }

    #[inline]
    async fn get_object<RETURN, KEY, PARSER>(
        &self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> Result<RETURN, Self::Error>
    where
        RETURN: DeserializeOwned + Send + Sync,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString,
    {
        let object = self.data.get(&key_with_parser.key().name());

        object.map_or_else(
            || Err(MemoryError::NotExistsObject(key_with_parser.key().name())),
            |content| parse_memory_object(content, key_with_parser),
        )
    }

    #[inline]
    async fn list_objects(&self, prefix: &str) -> Result<ListKeyObjects, Self::Error> {
        let delimiter = '/';
        let prefix_len = prefix.len();
        let objects = self
            .data
            .iter()
            .filter(|&(key, _)| key.starts_with(prefix))
            .filter_map(|(key, _)| {
                let (_, radical) = key.split_at(prefix_len);
                let radical_key = radical.split_once(delimiter);

                match radical_key {
                    None => Some(key.to_owned()),
                    Some((radical_without_suffix, _)) => {
                        if prefix.is_empty() {
                            Some(format!("{radical_without_suffix}{delimiter}"))
                        } else if radical_without_suffix.is_empty() {
                            None
                        } else {
                            Some(format!("{prefix}{radical_without_suffix}{delimiter}"))
                        }
                    }
                }
            })
            .collect();

        // TODO: Limit to 1000 keys
        Ok(objects)
    }
}

#[expect(clippy::single_call_fn, reason = "code readability")]
fn parse_memory_object<RETURN, KEY, PARSER>(
    content: &[u8],
    key_with_parser: &KeyWithParser<KEY, PARSER>,
) -> Result<RETURN, MemoryError>
where
    RETURN: DeserializeOwned + Send + Sync,
    KEY: KeyWhere,
    PARSER: ParserWhere,
    <PARSER as Parser>::Error: ToString,
{
    let object = key_with_parser
        .parser()
        .deserialize_value::<RETURN>(content)
        .map_err(|err| MemoryError::Serde {
            operation: "parse_memory_object".to_owned(),
            key: key_with_parser.key().name(),
            internal: err.to_string(),
        })?;

    Ok(object)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Key;

    enum TestKey {
        One,
        Long,
        Long2,
        VeryLong,
    }

    impl Key for TestKey {
        fn name(&self) -> String {
            match *self {
                TestKey::One => "one".to_owned(),
                TestKey::Long => "long/qux".to_owned(),
                TestKey::Long2 => "long/baz".to_owned(),
                TestKey::VeryLong => "long/verylong/buz".to_owned(),
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
            vec!["one".to_string()].into_iter().collect(),
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
