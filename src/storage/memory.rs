use gxhash::HashMap;
use serde::de::DeserializeOwned;

use super::{KeyWhere, ListKeyObjects, MemoryError, ParserWhere, Storage};
use crate::{KeyWithParser, Parser};

#[derive(Default)]
pub struct Memory {
    data: HashMap<String, Vec<u8>>,
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
            Ok(res) => self.put_bytes(res, key_with_parser).await,
            Err(err) => Err(MemoryError::Serde {
                operation: "put_object".to_owned(),
                key: key_with_parser.key().name(),
                internal: err.to_string(),
            }),
        }
    }

    #[inline]
    async fn put_bytes<KEY, PARSER>(
        &mut self,
        value: Vec<u8>,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> Result<&Self, Self::Error>
    where
        KEY: KeyWhere,
        PARSER: ParserWhere,
    {
        self.data.insert(key_with_parser.key().name(), value);
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
        let prefix_len = prefix.len();
        let objects = self
            .data
            .iter()
            .filter(|&(key, _)| key.starts_with(prefix))
            .map(|(key, _)| {
                let (_, radical) = key.split_at(prefix_len);
                let radical_key = radical.split_once('/');

                let key_without_suffix = match radical_key {
                    None => key.to_owned(),
                    Some((radical_without_suffix, _)) => {
                        format!("{prefix}/{radical_without_suffix}/")
                    }
                };

                Some(key_without_suffix)
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
