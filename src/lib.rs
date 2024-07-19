pub mod s3;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Builder;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::{AggregatedBytes, ByteStream};
use aws_sdk_s3::Client;
use core::fmt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::env;

#[derive(Debug)]
pub enum S3Error {
    Serde {
        operation: String,
        key: Key,
        internal: String,
    },
    S3Bucket {
        operation: String,
        bucket: String,
        internal: String,
    },
    S3Object {
        operation: String,
        key: Key,
        internal: String,
    },
    Query(String),
    NotExistsBucket(String),
    NotExistsObject(Key),
    EnvConfig(String),
}

#[derive(Debug, Clone, Serialize)]
pub enum Key {
    LoginWithUniqueCode,
}

impl fmt::Display for Key {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match *self {
            Self::LoginWithUniqueCode => "LoginWithUniqueCode",
        };
        write!(formatter, "{name}")
    }
}

impl From<&Key> for String {
    fn from(value: &Key) -> Self {
        let base = match *value {
            Key::LoginWithUniqueCode => "login-with-code".to_owned(),
        };

        format!("{base}.json")
    }
}
#[derive(Debug, Clone)]
pub struct S3 {
    inner: Client,
    bucket: String,
}

impl S3 {
    pub async fn new(bucket: String) -> Result<Self, S3Error> {
        Ok(Self {
            inner: create_client().await?,
            bucket,
        })
    }

    pub async fn put_object<T: Serialize + Send>(&self, key: Key, body: T) -> Result<(), S3Error> {
        let body = serde_json::to_vec(&body);

        match body {
            Ok(body) => self.put_bytes(body, key).await,
            Err(err) => Err(S3Error::S3Object {
                operation: "put_object".to_owned(),
                key,
                internal: err.to_string(),
            }),
        }
    }

    async fn put_bytes(&self, body: Vec<u8>, key: Key) -> Result<(), S3Error> {
        self.inner
            .put_object()
            .bucket(&self.bucket)
            .key(key.to_string())
            .body(ByteStream::from(body))
            .set_content_type(Some("application/json".to_owned()))
            .send()
            .await
            .map(|_| ())
            .map_err(|err| S3Error::S3Object {
                operation: "put_bytes".to_owned(),
                key,
                internal: err.to_string(),
            })
    }

    pub async fn get_object<T: DeserializeOwned + Send>(&self, key: Key) -> Result<T, S3Error> {
        let body = self
            .inner
            .get_object()
            .bucket(&self.bucket)
            .key(key.to_string())
            .send()
            .await;

        match body {
            Ok(body) => parse_s3_object(body, key).await,
            Err(err) => Err(S3Error::S3Object {
                operation: "get_object".to_owned(),
                key,
                internal: err.to_string(),
            }),
        }
    }
}

async fn parse_s3_object<T: DeserializeOwned + Send>(
    body: GetObjectOutput,
    key: Key,
) -> Result<T, S3Error> {
    if body.content_length().unwrap_or_default() == 0 {
        Err(S3Error::NotExistsObject(key))
    } else {
        let content = body.body.collect().await;

        match content {
            Ok(content) => parse_aggregated_bytes(content, key),
            Err(err) => Err(S3Error::S3Object {
                operation: "parse_s3_object".to_owned(),
                key,
                internal: err.to_string(),
            }),
        }
    }
}

fn parse_aggregated_bytes<T: DeserializeOwned + Send>(
    content: AggregatedBytes,
    key: Key,
) -> Result<T, S3Error> {
    let object = serde_json::from_slice(&content.to_vec()).map_err(|err| S3Error::Serde {
        operation: "parse_aggregated_bytes".to_owned(),
        key,
        internal: err.to_string(),
    })?;

    Ok(object)
}

async fn create_client() -> Result<Client, S3Error> {
    let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let config = Builder::from(&sdk_config)
        .endpoint_url(env::var("S3_ENDPOINT").map_err(|err| S3Error::EnvConfig(err.to_string()))?)
        .region(Region::new(
            env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_owned()),
        ))
        .force_path_style(true)
        .build();
    Ok(aws_sdk_s3::Client::from_conf(config))
}
