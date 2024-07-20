#![expect(clippy::implicit_return, reason = "return is idiomatic")]
#![expect(clippy::exhaustive_enums, reason = "break enum is acceptable")]
#![expect(
    clippy::question_mark_used,
    reason = "not use macro to attach additional information for result"
)]
#![expect(clippy::missing_docs_in_private_items, reason = "unstable lib")]
#![expect(clippy::missing_errors_doc, reason = "unstable api")]

use serde::Serialize;

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
    Query(String),
    NotExistsBucket(String),
    NotExistsObject(String),
    EnvConfig(String),
}

#[derive(Debug, Clone, Serialize)]
pub enum SampleKey {
    LoginWithUniqueCode,
}

impl Key for SampleKey {
    type Error = serde_json::Error;

    #[inline]
    fn name<'src>(&self) -> &'src str {
        match *self {
            Self::LoginWithUniqueCode => "login-with-code",
        }
    }

    #[inline]
    fn serialize_value<T>(&self, value: &T) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send,
    {
        serde_json::to_vec(value)
    }

    #[inline]
    fn deserialize_value<T>(&self, content: &[u8]) -> Result<T, Self::Error>
    where
        T: for<'content> serde::Deserialize<'content>,
    {
        serde_json::from_slice(content)
    }

    #[inline]
    fn mime(&self) -> String {
        "application/json".to_owned()
    }
}

pub trait Key {
    type Error;

    fn name<'src>(&self) -> &'src str;

    fn serialize_value<T>(&self, value: &T) -> Result<Vec<u8>, Self::Error>
    where
        T: Serialize + Send;

    fn deserialize_value<T>(&self, content: &[u8]) -> Result<T, Self::Error>
    where
        T: for<'content> serde::Deserialize<'content>;

    fn mime(&self) -> String;
}
