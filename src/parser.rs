use serde::Serialize;

use crate::Parser;

#[derive(Default)]
pub struct Json;

impl Parser for Json {
    type Error = serde_json::Error;

    #[inline]
    fn serialize_value<VALUE>(&self, value: &VALUE) -> Result<Vec<u8>, Self::Error>
    where
        VALUE: Serialize + Send,
    {
        serde_json::to_vec(value)
    }

    #[inline]
    fn deserialize_value<RETURN>(&self, content: &[u8]) -> Result<RETURN, Self::Error>
    where
        RETURN: for<'content> serde::Deserialize<'content>,
    {
        serde_json::from_slice(content)
    }

    #[inline]
    fn mime(&self) -> String {
        "application/json".to_owned()
    }
}
