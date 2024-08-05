use std::path::Path;
use std::{env, fs};

use directories::ProjectDirs;
use semver::{BuildMetadata, Version};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::storage::key::Key;
use crate::storage::key_with_parser::KeyWithParser;
use crate::storage::parser::Json;
use crate::storage::sink::memory::Memory;
use crate::storage::{MemoryError, Storage, ValueWhere};
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
pub struct Builder {
    instance_id: Option<String>,
}

impl Builder {
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
        let key = format!("{}_NEGENTROPY_INSTANCE_ID", prefix);
        Self {
            instance_id: env::var(key).ok().or(self.instance_id),
        }
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

    #[inline]
    pub async fn build(self) -> Result<Instance<Memory>, MemoryError> {
        let instance_id = self
            .instance_id
            .and_then(|instance_id| Uuid::parse_str(&instance_id).ok())
            .ok_or_else(|| BuilderError::MissingVar("instance_id".to_owned()))
            .unwrap();
        Instance::new(Memory::default(), instance_id).await
    }
}

pub struct Instance<STORAGE: Storage + Send + Sync> {
    storage: STORAGE,
    instance_id: Uuid,
}

impl<STORAGE> Instance<STORAGE>
where
    STORAGE: Storage + Send + Sync,
    <STORAGE as Storage>::Error: Send + Sync,
{
    #[inline]
    pub async fn new(storage: STORAGE, instance_id: Uuid) -> Result<Self, STORAGE::Error> {
        let instance = Self {
            storage,
            instance_id,
        };

        instance.welcome().await?.initialize().await
    }

    async fn welcome(mut self) -> Result<Self, STORAGE::Error> {
        let welcome = Welcome::default();
        let key_with_parser = KeyWithParser::new(&InstanceKey::Welcome, &Json);
        self.storage
            .put_object_if_not_exists(&key_with_parser, &welcome)
            .await?;
        Ok(self)
    }

    async fn initialize(mut self) -> Result<Self, STORAGE::Error> {
        let initialize = Initialize;
        let key = &InstanceKey::Initialize(self.instance_id.to_string());
        let key_with_parser = KeyWithParser::new(key, &Json);
        self.storage
            .put_object_if_not_exists(&key_with_parser, &initialize)
            .await?;
        Ok(self)
    }

    pub async fn put_object<KEY, VALUE>(
        &mut self,
        key: &KEY,
        value: &VALUE,
    ) -> Result<&Self, STORAGE::Error>
    where
        KEY: Key + Send + Sync,
        VALUE: ValueWhere,
        <STORAGE as Storage>::Error: std::fmt::Debug,
    {
        self.storage
            .put_object_if_not_exists(&KeyWithParser::new(key, &Json), value)
            .await
            .unwrap();

        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sink::memory::Memory;

    #[tokio::test]
    async fn welcome() {
        let memory = Memory::default();
        let instance = Instance::new(memory, Uuid::new_v4()).await.unwrap();
        let key_with_parser = KeyWithParser::new(&InstanceKey::Welcome, &Json);
        instance
            .storage
            .get_object::<Welcome, _, _>(&key_with_parser)
            .await
            .unwrap();
    }
}
