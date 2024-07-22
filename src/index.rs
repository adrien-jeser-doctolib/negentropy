use semver::{BuildMetadata, Version};
use serde::{Deserialize, Serialize};

use crate::{s3::S3, Key};

#[derive(Debug, Clone, Serialize)]
pub enum IndexKey {
    Welcome,
}

impl Key for IndexKey {
    type Error = serde_json::Error;

    #[inline]
    fn name<'src>(&self) -> &'src str {
        match *self {
            Self::Welcome => "welcome",
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

#[derive(Serialize, Deserialize)]
pub struct Welcome {
    version: Version,
}

impl Default for Welcome {
    #[inline]
    fn default() -> Self {
        Self {
            version: Version {
                major: env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap_or_default(),
                minor: env!("CARGO_PKG_VERSION_MINOR").parse().unwrap_or_default(),
                patch: env!("CARGO_PKG_VERSION_PATCH").parse().unwrap_or_default(),
                pre: env!("CARGO_PKG_VERSION_PRE").parse().unwrap_or_default(),
                build: BuildMetadata::EMPTY,
            },
        }
    }
}

#[inline]
pub async fn always_welcome(s3: &S3) {
    let welcome = Welcome::default();
    s3.put_object(&IndexKey::Welcome, &welcome).await.unwrap();
}
