use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Builder;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::{AggregatedBytes, ByteStream};
use aws_sdk_s3::Client;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::env;

use crate::{Key, S3Error};

#[derive(Debug, Clone)]
pub struct S3 {
    inner: Client,
    bucket: String,
}

impl S3 {
    #[inline]
    pub async fn new(bucket: String) -> Result<Self, S3Error> {
        Ok(Self {
            inner: create_client().await?,
            bucket,
        })
    }

    #[inline]
    pub async fn put_object<T, K>(&self, key: &K, value: &T) -> Result<(), S3Error>
    where
        T: Serialize + Send + Sync,
        K: Key + Send + Sync,
        <K as Key>::Error: ToString + Send + Sync,
    {
        let serialize = key.serialize_value(value);

        match serialize {
            Ok(res) => self.put_bytes(res, key).await,
            Err(err) => Err(S3Error::S3Object {
                operation: "put_object".to_owned(),
                key: key.name().to_owned(),
                internal: err.to_string(),
            }),
        }
    }

    async fn put_bytes<K>(&self, value: Vec<u8>, key: &K) -> Result<(), S3Error>
    where
        K: Key + Send + Sync,
    {
        self.inner
            .put_object()
            .bucket(&self.bucket)
            .key(key.name())
            .body(ByteStream::from(value))
            .set_content_type(Some(key.mime()))
            .send()
            .await
            .map(|_| ())
            .map_err(|err| S3Error::S3Object {
                operation: "put_bytes".to_owned(),
                key: key.name().to_owned(),
                internal: err.to_string(),
            })
    }

    #[inline]
    pub async fn get_object<T, K>(&self, key: &K) -> Result<T, S3Error>
    where
        T: DeserializeOwned + Send + Sync,
        K: Key + Send + Sync,
        <K as Key>::Error: ToString,
    {
        let object = self
            .inner
            .get_object()
            .bucket(&self.bucket)
            .key(key.name())
            .send()
            .await;

        match object {
            Ok(object_output) => parse_s3_object(object_output, key).await,
            Err(err) => Err(S3Error::S3Object {
                operation: "get_object".to_owned(),
                key: key.name().to_owned(),
                internal: err.to_string(),
            }),
        }
    }
}

#[expect(clippy::single_call_fn, reason = "code readability")]
async fn parse_s3_object<T, K>(object: GetObjectOutput, key: &K) -> Result<T, S3Error>
where
    T: DeserializeOwned + Send + Sync,
    K: Key + Send + Sync,
    <K as Key>::Error: ToString,
{
    if object.content_length().unwrap_or_default() == 0 {
        Err(S3Error::NotExistsObject(key.name().to_owned()))
    } else {
        let try_decoding = object.body.collect().await;

        match try_decoding {
            Ok(content) => parse_aggregated_bytes(content, key),
            Err(err) => Err(S3Error::S3Object {
                operation: "parse_s3_object".to_owned(),
                key: key.name().to_owned(),
                internal: err.to_string(),
            }),
        }
    }
}

#[expect(clippy::single_call_fn, reason = "code readability")]
fn parse_aggregated_bytes<T, K>(content: AggregatedBytes, key: &K) -> Result<T, S3Error>
where
    T: DeserializeOwned + Send + Sync,
    K: Key + Send + Sync,
    <K as Key>::Error: ToString,
{
    let object = key
        .deserialize_value::<T>(&content.to_vec())
        .map_err(|err| S3Error::Serde {
            operation: "parse_aggregated_bytes".to_owned(),
            key: key.name().to_owned(),
            internal: err.to_string(),
        })?;

    Ok(object)
}

#[expect(clippy::single_call_fn, reason = "code readability")]
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
