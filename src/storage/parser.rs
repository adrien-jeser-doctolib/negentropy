use serde::{Deserialize, Serialize};

use super::ParserError;
use crate::storage::ValueWhere;

pub trait ParserCopy {
    fn serialize_value<VALUE>(&self, value: &VALUE) -> Result<Vec<u8>, ParserError>
    where
        VALUE: ValueWhere;

    fn deserialize_value<CONTENT>(&self, content: &[u8]) -> Result<CONTENT, ParserError>
    where
        CONTENT: for<'content> Deserialize<'content>;

    fn mime(&self) -> String;
}

#[derive(Default)]
pub struct Json;

impl ParserCopy for Json {
    #[inline]
    fn serialize_value<VALUE>(&self, value: &VALUE) -> Result<Vec<u8>, ParserError>
    where
        VALUE: Serialize + Send,
    {
        serde_json::to_vec(value).map_err(|err| ParserError::Serde {
            internal: err.to_string(),
        })
    }

    #[inline]
    fn deserialize_value<RETURN>(&self, content: &[u8]) -> Result<RETURN, ParserError>
    where
        RETURN: for<'content> Deserialize<'content>,
    {
        serde_json::from_slice(content).map_err(|err| ParserError::Serde {
            internal: err.to_string(),
        })
    }

    #[inline]
    fn mime(&self) -> String {
        "application/json".to_owned()
    }
}
