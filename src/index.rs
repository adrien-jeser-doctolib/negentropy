use crate::{s3::S3, IndexKey, S3Error};
use semver::{BuildMetadata, Version};
use serde::{Deserialize, Serialize};

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
pub async fn always_welcome(s3: &S3) -> Result<Welcome, S3Error> {
    let welcome = Welcome::default();
    s3.put_object(&IndexKey::Welcome, &welcome).await?;
    Ok(welcome)
}
