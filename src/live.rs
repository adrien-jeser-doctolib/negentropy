use semver::{BuildMetadata, Version};
use serde::{Deserialize, Serialize};

use crate::key_with_parser::KeyWithParser;
use crate::parser::Json;
use crate::storage::s3::S3;
use crate::storage::{S3Error, Storage};
use crate::LiveKey;

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
pub async fn welcome(s3: &mut S3) -> Result<Welcome, S3Error> {
    let welcome = Welcome::default();
    let key_with_parser = KeyWithParser::new(LiveKey::Welcome, Json);
    s3.put_object_if_not_exists(&key_with_parser, &welcome)
        .await?;
    Ok(welcome)
}
