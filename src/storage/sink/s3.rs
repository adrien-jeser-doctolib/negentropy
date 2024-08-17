use std::env;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Builder;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::primitives::{AggregatedBytes, ByteStream};
use aws_sdk_s3::Client;
use serde::de::DeserializeOwned;

use crate::storage::direct::DKeyWithParserCopy;
use crate::storage::{DKeyWhere, ListKeyObjects, ParserWhere, S3Error, SinkCopy, ValueWhere};

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

    async fn exists_inner(&self, key: String) -> Result<bool, S3Error> {
        let head_object = self
            .inner
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await;

        match head_object {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(err))
                if matches!(err.err(), &HeadObjectError::NotFound(_)) =>
            {
                Ok(false)
            }
            Err(err) => Err(S3Error::S3Exists {
                operation: "exists".to_owned(),
                key,
                internal: err.to_string(),
            }),
        }
    }

    async fn put_bytes_inner(
        &mut self,
        key: String,
        mime: String,
        value: Vec<u8>,
    ) -> Result<(), S3Error> {
        self.inner
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(value))
            .set_content_type(Some(mime))
            .send()
            .await
            .map_err(|err| S3Error::S3Object {
                operation: "put_bytes".to_owned(),
                key,
                internal: err.to_string(),
            })?;

        Ok(())
    }

    async fn list_objects_inner(&self, prefix: &str) -> Result<ListKeyObjects, S3Error> {
        let list = self
            .inner
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .set_delimiter(Some("/".to_owned()))
            .send()
            .await;

        match list {
            Ok(list_output) => handle_list_objects(list_output),
            Err(err) => Err(S3Error::S3List {
                operation: "list_objects".to_owned(),
                prefix: prefix.to_owned(),
                internal: Some(err.to_string()),
            }),
        }
    }

    async fn put_object_inner<VALUE, F>(
        &mut self,
        key: String,
        mime: String,
        value: &VALUE,
        f: F,
    ) -> Result<(), S3Error>
    where
        F: Fn(&VALUE) -> Result<Vec<u8>, S3Error>,
    {
        let serialize = f(value);

        match serialize {
            Ok(res) => self.put_bytes_inner(key, mime, res).await,
            Err(err) => Err(S3Error::S3Object {
                operation: "put_object".to_owned(),
                key,
                internal: err.to_string(),
            }),
        }
    }

    async fn get_object_inner<RETURN, F>(
        &self,
        key: String,
        f: F,
    ) -> Result<Option<RETURN>, S3Error>
    where
        RETURN: Send + Sync,
        F: Fn(&[u8]) -> Result<RETURN, S3Error>,
    {
        let object = self
            .inner
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await;

        match object {
            Ok(object_output) => parse_s3_object(object_output, key, f).await,
            Err(SdkError::ServiceError(err))
                if matches!(err.err(), &GetObjectError::NoSuchKey(_)) =>
            {
                Ok(None)
            }
            Err(err) => Err(S3Error::S3Object {
                operation: "get_object".to_owned(),
                key,
                internal: err.to_string(),
            }),
        }
    }
}

impl SinkCopy for S3 {
    type Error = S3Error;

    #[inline]
    async fn exists_copy<DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParserCopy<'_, DKEY, PARSER>,
    ) -> Result<bool, Self::Error>
    where
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        self.exists_inner(key_with_parser.key().name()).await
    }

    #[inline]
    async fn put_object_copy<VALUE, DKEY, PARSER>(
        &mut self,
        key_with_parser: &DKeyWithParserCopy<'_, DKEY, PARSER>,
        value: &VALUE,
    ) -> Result<(), Self::Error>
    where
        VALUE: ValueWhere,
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        self.put_object_inner(
            key_with_parser.key().name(),
            key_with_parser.parser().mime(),
            value,
            |value| Ok(key_with_parser.parser().serialize_value(value)?),
        )
        .await
    }

    #[inline]
    async fn put_bytes_copy<DKEY>(
        &mut self,
        key: &DKEY,
        mime: String,
        value: Vec<u8>,
    ) -> Result<(), Self::Error>
    where
        DKEY: DKeyWhere,
    {
        self.put_bytes_inner(key.name(), mime, value).await
    }

    #[inline]
    async fn get_object_copy<RETURN, DKEY, PARSER>(
        &self,
        key_with_parser: &DKeyWithParserCopy<'_, DKEY, PARSER>,
    ) -> Result<Option<RETURN>, Self::Error>
    where
        RETURN: DeserializeOwned + Send + Sync,
        DKEY: DKeyWhere,
        PARSER: ParserWhere,
    {
        self.get_object_inner(key_with_parser.key().name(), |content| {
            Ok(key_with_parser.parser().deserialize_value(content)?)
        })
        .await
    }

    #[inline]
    async fn list_objects_copy(&self, prefix: &str) -> Result<ListKeyObjects, Self::Error> {
        self.list_objects_inner(prefix).await
    }
}

#[expect(clippy::single_call_fn, reason = "code readability")]
fn handle_list_objects(list: ListObjectsV2Output) -> Result<ListKeyObjects, S3Error> {
    list.contents
        .map_or(Err(S3Error::S3ListHandle), |contents| {
            Ok(contents
                .into_iter()
                .filter_map(|content| content.key)
                .collect())
        })
}

#[expect(clippy::single_call_fn, reason = "code readability")]
async fn parse_s3_object<RETURN, F>(
    object: GetObjectOutput,
    key: String,
    f: F,
) -> Result<Option<RETURN>, S3Error>
where
    RETURN: Send + Sync,
    F: Fn(&[u8]) -> Result<RETURN, S3Error>,
{
    if object.content_length().unwrap_or_default() == 0 {
        Ok(None)
    } else {
        let try_decoding = object.body.collect().await;

        match try_decoding {
            Ok(content) => Ok(Some(parse_aggregated_bytes(content, f)?)),
            Err(err) => Err(S3Error::S3Object {
                operation: "parse_s3_object".to_owned(),
                key,
                internal: err.to_string(),
            }),
        }
    }
}

#[expect(clippy::single_call_fn, reason = "code readability")]
fn parse_aggregated_bytes<RETURN, F>(content: AggregatedBytes, f: F) -> Result<RETURN, S3Error>
where
    RETURN: Send + Sync,
    F: Fn(&[u8]) -> Result<RETURN, S3Error>,
{
    let object = f(&content.to_vec())?;
    Ok(object)
}

#[expect(clippy::single_call_fn, reason = "code readability")]
async fn create_client() -> Result<Client, S3Error> {
    let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let config = Builder::from(&sdk_config)
        .endpoint_url(
            env::var("S3_ENDPOINT")
                .map_err(|err| S3Error::EnvConfig(format!("S3_ENDPOINT {err}")))?,
        )
        .region(Region::new(
            env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_owned()),
        ))
        .force_path_style(true)
        .build();
    Ok(aws_sdk_s3::Client::from_conf(config))
}
