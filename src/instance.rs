use core::fmt::Debug;
use std::path::Path;
use std::{env, fs};

use directories::ProjectDirs;
use semver::{BuildMetadata, Version};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::storage::direct::{DKey, DKeyWithParserCopy};
use crate::storage::parser_copy::Json;
use crate::storage::{CacheCopy, ValueWhere};
use crate::InstanceKey;

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

#[derive(Serialize, Deserialize)]
pub struct Initialize;

#[derive(Debug)]
pub enum BuilderError {
    MissingVar(String),
    Serde(String),
}

#[derive(Default, Serialize, Deserialize)]
pub struct Configuration {
    pub instance_id: Option<Uuid>,
}

impl Configuration {
    #[inline]
    pub fn load(
        self,
        qualifier: &str,
        organization: &str,
        application: &str,
    ) -> Result<Self, BuilderError> {
        let prefix = format!(
            "{}_{}_{}",
            qualifier.to_uppercase(),
            organization.to_uppercase(),
            application.to_uppercase()
        );

        if let Some(project_dirs) = ProjectDirs::from(qualifier, organization, application) {
            Ok(self
                .load_from_file(project_dirs.config_dir().join("negentropy.toml").as_path())?
                .load_from_env(&prefix))
        } else {
            Ok(self.load_from_env(&prefix))
        }
    }

    #[inline]
    #[must_use]
    pub fn load_from_env(self, prefix: &str) -> Self {
        let key = format!("{prefix}_NEGENTROPY_INSTANCE_ID");
        let instance_id = env::var(key)
            .ok()
            .and_then(|value| Uuid::parse_str(&value).ok());
        Self { instance_id }
    }

    #[inline]
    pub fn load_from_file(self, path: &Path) -> Result<Self, BuilderError> {
        let read = fs::read_to_string(path);

        if let Ok(content) = read {
            let config: Self =
                toml::from_str(&content).map_err(|err| BuilderError::Serde(err.to_string()))?;

            Ok(Self {
                instance_id: config.instance_id.or(self.instance_id),
            })
        } else {
            Ok(self)
        }
    }
}

pub struct Instance<CACHE: CacheCopy + Send + Sync> {
    storage: CACHE,
    configuration: Configuration,
}

impl<CACHE> Instance<CACHE>
where
    CACHE: CacheCopy + Send + Sync,
    <CACHE as CacheCopy>::Error: Send + Sync,
{
    #[inline]
    pub async fn new(storage: CACHE, configuration: Configuration) -> Result<Self, CACHE::Error> {
        let instance = Self {
            storage,
            configuration,
        };

        instance.welcome().await?.initialize().await
    }

    async fn welcome(mut self) -> Result<Self, CACHE::Error> {
        let welcome = Welcome::default();
        let key_with_parser = DKeyWithParserCopy::new(&InstanceKey::Welcome, &Json);
        self.storage
            .put_object_if_not_exists_copy(&key_with_parser, &welcome)
            .await?;
        Ok(self)
    }

    async fn initialize(mut self) -> Result<Self, CACHE::Error> {
        let initialize = Initialize;
        let key = &InstanceKey::Initialize(
            self.configuration
                .instance_id
                .unwrap_or_default()
                .to_string(),
        );
        let key_with_parser = DKeyWithParserCopy::new(key, &Json);
        self.storage
            .put_object_if_not_exists_copy(&key_with_parser, &initialize)
            .await?;
        Ok(self)
    }

    #[inline]
    pub async fn put_object<DKEY, VALUE>(
        &mut self,
        key: &DKEY,
        value: &VALUE,
    ) -> Result<&Self, CACHE::Error>
    where
        DKEY: DKey + Send + Sync,
        VALUE: ValueWhere,
        <CACHE as CacheCopy>::Error: Debug,
    {
        self.storage
            .put_object_if_not_exists_copy(&DKeyWithParserCopy::new(key, &Json), value)
            .await?;

        Ok(self)
    }

    #[inline]
    pub fn cache(&mut self) -> &mut CACHE {
        &mut self.storage
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::storage::cache::lru::Lru;
    use crate::storage::sink::memory::Memory;

    #[tokio::test]
    async fn welcome() {
        let memory = Memory::default();
        let lru = Lru::new(NonZeroUsize::new(10).unwrap(), memory);
        let builder = Configuration::default();
        let mut instance = Instance::new(lru, builder).await.unwrap();
        let key_with_parser = DKeyWithParserCopy::new(&InstanceKey::Welcome, &Json);
        instance
            .storage
            .get_object_copy::<Welcome, _, _>(&key_with_parser)
            .await
            .unwrap();
    }
}
