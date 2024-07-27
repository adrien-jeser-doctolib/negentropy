use super::{KeyWhere, MemoryError, ParserWhere, Storage};
use crate::{KeyWithParser, Parser};
use gxhash::HashMap;
use serde::de::DeserializeOwned;

#[derive(Default)]
pub struct Memory {
    data: HashMap<String, Vec<u8>>,
}

impl Storage for Memory {
    type Error = MemoryError;

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

    async fn put_object<VALUE, KEY, PARSER>(
        &mut self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
        value: &VALUE,
    ) -> Result<&Self, Self::Error>
    where
        VALUE: super::ValueWhere,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString,
    {
        let serialize = key_with_parser.parser().serialize_value(value);

        match serialize {
            Ok(res) => self.put_bytes(res, key_with_parser).await,
            Err(_) => todo!(),
        }
    }

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
        let object = self.data.get(&key_with_parser.key().name()).unwrap();
        key_with_parser
            .parser()
            .deserialize_value::<RETURN>(object)
            .map_err(|err| MemoryError {})
    }

    async fn list_objects(&self, prefix: &str) -> Result<super::ListKeyObjects, Self::Error> {
        todo!()
    }
}
