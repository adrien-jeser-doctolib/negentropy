use super::{KeyWhere, ParserWhere, Storage};
use crate::KeyWithParser;
use gxhash::{HashMap, HashMapExt};

pub struct Memory {
    data: HashMap<String, Vec<u8>>,
}

impl Storage for Memory {
    type Error = ();

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
        &self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
        value: &VALUE,
    ) -> Result<&Self, Self::Error>
    where
        VALUE: super::ValueWhere,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as crate::Parser>::Error: ToString,
    {
        let serialize = key_with_parser.parser().serialize_value(value);

        match serialize {
            Ok(res) => self.put_bytes(res, key_with_parser).await,
            Err(_) => todo!(),
        }
    }

    async fn put_bytes<KEY, PARSER>(
        &self,
        value: Vec<u8>,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> Result<&Self, Self::Error>
    where
        KEY: KeyWhere,
        PARSER: ParserWhere,
    {
        // self.data.insert(key_with_parser.key().name(), value);
        Ok(&self)
    }

    async fn get_object<RETURN, KEY, PARSER>(
        &self,
        key_with_parser: &KeyWithParser<KEY, PARSER>,
    ) -> Result<RETURN, Self::Error>
    where
        RETURN: serde::de::DeserializeOwned + Send + Sync,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as crate::Parser>::Error: ToString,
    {
        todo!()
    }

    fn list_objects(
        &self,
        prefix: &str,
    ) -> impl futures::Future<Output = Result<super::ListKeyObjects, Self::Error>> {
        todo!()
    }
}
