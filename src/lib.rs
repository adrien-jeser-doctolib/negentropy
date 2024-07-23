#![expect(clippy::implicit_return, reason = "return is idiomatic")]
#![expect(clippy::exhaustive_enums, reason = "break enum is acceptable")]
#![expect(
    clippy::question_mark_used,
    reason = "not use macro to attach additional information for result"
)]
#![expect(clippy::missing_docs_in_private_items, reason = "unstable lib")]
#![expect(clippy::missing_errors_doc, reason = "unstable api")]
#![expect(clippy::ref_patterns, reason = "ref is idiomatic")]
#![expect(clippy::missing_trait_methods, reason = "add only specific trait")]

use core::{future::Future, option::Option};
use serde::{de::DeserializeOwned, Serialize};

pub mod live;
pub mod s3;

#[derive(Debug)]
pub enum S3Error {
    Serde {
        operation: String,
        key: String,
        internal: String,
    },
    S3Bucket {
        operation: String,
        bucket: String,
        internal: String,
    },
    S3Object {
        operation: String,
        key: String,
        internal: String,
    },
    S3List {
        operation: String,
        prefix: String,
        internal: Option<String>,
    },
    S3Exists {
        operation: String,
        key: String,
        internal: String,
    },
    S3ListHandle,
    NotExistsObject(String),
    EnvConfig(String),
}

pub trait Key {
    type Error;

    fn name(&self) -> String;

    fn serialize_value<VALUE>(&self, value: &VALUE) -> Result<Vec<u8>, Self::Error>
    where
        VALUE: Serialize + Send;

    fn deserialize_value<CONTENT>(&self, content: &[u8]) -> Result<CONTENT, Self::Error>
    where
        CONTENT: for<'content> serde::Deserialize<'content>;

    fn mime(&self) -> String;
}

pub trait Storage {
    fn exists<KEY>(&self, key: &KEY) -> impl Future<Output = Result<bool, S3Error>>
    where
        KEY: Key + Send + Sync;

    #[inline]
    fn put_object_if_not_exists<KEY, VALUE>(
        &self,
        key: &KEY,
        value: &VALUE,
    ) -> impl Future<Output = Result<bool, S3Error>>
    where
        KEY: Key + Send + Sync,
        <KEY as Key>::Error: ToString + Send + Sync,
        VALUE: Serialize + Send + Sync,
        Self: Sync,
    {
        async {
            if self.exists(key).await? {
                Ok(false)
            } else {
                self.put_object(key, value).await?;
                Ok(true)
            }
        }
    }

    #[inline]
    fn put_object<VALUE, KEY>(
        &self,
        key: &KEY,
        value: &VALUE,
    ) -> impl Future<Output = Result<&Self, S3Error>>
    where
        VALUE: Serialize + Send + Sync,
        KEY: Key + Send + Sync,
        <KEY as Key>::Error: ToString + Send + Sync,
    {
        async {
            let serialize = key.serialize_value(value);

            match serialize {
                Ok(res) => self.put_bytes(res, key).await,
                Err(err) => Err(S3Error::S3Object {
                    operation: "put_object".to_owned(),
                    key: key.name(),
                    internal: err.to_string(),
                }),
            }
        }
    }

    fn put_bytes<KEY>(
        &self,
        value: Vec<u8>,
        key: &KEY,
    ) -> impl Future<Output = Result<&Self, S3Error>> + Send
    where
        KEY: Key + Send + Sync;

    fn get_object<RETURN, KEY>(&self, key: &KEY) -> impl Future<Output = Result<RETURN, S3Error>>
    where
        RETURN: DeserializeOwned + Send + Sync,
        KEY: Key + Send + Sync,
        <KEY as Key>::Error: ToString;

    fn list_objects(
        &self,
        prefix: &str,
    ) -> impl Future<Output = Result<Vec<Option<String>>, S3Error>>;
}

#[derive(Debug, Clone, Serialize)]
pub enum LiveKey {
    Welcome,
    Alive(String),
}

impl Key for LiveKey {
    type Error = serde_json::Error;

    #[inline]
    fn name(&self) -> String {
        match *self {
            Self::Welcome => "live/welcome".to_owned(),
            Self::Alive(ref id) => format!("live/alive-{id}"),
        }
    }

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
