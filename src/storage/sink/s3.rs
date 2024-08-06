use std::env;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Builder;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::primitives::{AggregatedBytes, ByteStream};
use aws_sdk_s3::Client;
use serde::de::DeserializeOwned;

use crate::storage::key_with_parser::KeyWithParser;
use crate::storage::parser::Parser;
use crate::storage::{KeyWhere, ListKeyObjects, ParserWhere, S3Error, Storage, ValueWhere};

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
}

impl Storage for S3 {
    type Error = S3Error;

    #[inline]
    async fn exists<KEY, PARSER>(
        &self,
        key_with_parser: &KeyWithParser<'_, KEY, PARSER>,
    ) -> Result<bool, Self::Error>
    where
        KEY: KeyWhere,
        PARSER: ParserWhere,
    {
        let head_object = self
            .inner
            .head_object()
            .bucket(&self.bucket)
            .key(key_with_parser.key().name())
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
                key: key_with_parser.key().name(),
                internal: err.to_string(),
            }),
        }
    }

    #[inline]
    async fn put_object<VALUE, KEY, PARSER>(
        &mut self,
        key_with_parser: &KeyWithParser<'_, KEY, PARSER>,
        value: &VALUE,
    ) -> Result<&Self, Self::Error>
    where
        VALUE: ValueWhere,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString + Send,
    {
        let serialize = key_with_parser.parser().serialize_value(value);

        match serialize {
            Ok(res) => {
                self.put_bytes(res, key_with_parser.key(), key_with_parser.parser().mime())
                    .await
            }
            Err(err) => Err(S3Error::S3Object {
                operation: "put_object".to_owned(),
                key: key_with_parser.key().name(),
                internal: err.to_string(),
            }),
        }
    }

    #[inline]
    async fn put_bytes<KEY>(
        &mut self,
        value: Vec<u8>,
        key: &KEY,
        mime: String,
    ) -> Result<&Self, Self::Error>
    where
        KEY: KeyWhere,
    {
        self.inner
            .put_object()
            .bucket(&self.bucket)
            .key(key.name())
            .body(ByteStream::from(value))
            .set_content_type(Some(mime.clone()))
            .send()
            .await
            .map_err(|err| S3Error::S3Object {
                operation: "put_bytes".to_owned(),
                key: key.name(),
                internal: err.to_string(),
            })?;

        Ok(self)
    }

    #[inline]
    async fn get_object<RETURN, KEY, PARSER>(
        &self,
        key_with_parser: &KeyWithParser<'_, KEY, PARSER>,
    ) -> Result<Option<RETURN>, Self::Error>
    where
        RETURN: DeserializeOwned + Send + Sync,
        KEY: KeyWhere,
        PARSER: ParserWhere,
        <PARSER as Parser>::Error: ToString,
    {
        let object = self
            .inner
            .get_object()
            .bucket(&self.bucket)
            .key(key_with_parser.key().name())
            .send()
            .await;

        match object {
            Ok(object_output) => parse_s3_object(object_output, key_with_parser).await,
            Err(err) => Err(S3Error::S3Object {
                operation: "get_object".to_owned(),
                key: key_with_parser.key().name(),
                internal: err.to_string(),
            }),
        }
    }

    #[inline]
    async fn list_objects(&self, prefix: &str) -> Result<ListKeyObjects, Self::Error> {
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
async fn parse_s3_object<RETURN, KEY, PARSER>(
    object: GetObjectOutput,
    key_with_parser: &KeyWithParser<'_, KEY, PARSER>,
) -> Result<Option<RETURN>, S3Error>
where
    RETURN: DeserializeOwned + Send + Sync,
    KEY: KeyWhere,
    PARSER: ParserWhere,
    <PARSER as Parser>::Error: ToString,
{
    if object.content_length().unwrap_or_default() == 0 {
        Ok(None)
    } else {
        let try_decoding = object.body.collect().await;

        match try_decoding {
            Ok(content) => parse_aggregated_bytes(content, key_with_parser),
            Err(err) => Err(S3Error::S3Object {
                operation: "parse_s3_object".to_owned(),
                key: key_with_parser.key().name(),
                internal: err.to_string(),
            }),
        }
    }
}

#[expect(clippy::single_call_fn, reason = "code readability")]
fn parse_aggregated_bytes<RETURN, KEY, PARSER>(
    content: AggregatedBytes,
    key_with_parser: &KeyWithParser<KEY, PARSER>,
) -> Result<RETURN, S3Error>
where
    RETURN: DeserializeOwned + Send + Sync,
    KEY: KeyWhere,
    PARSER: ParserWhere,
    <PARSER as Parser>::Error: ToString,
{
    let object = key_with_parser
        .parser()
        .deserialize_value::<RETURN>(&content.to_vec())
        .map_err(|err| S3Error::Serde {
            operation: "parse_aggregated_bytes".to_owned(),
            key: key_with_parser.key().name(),
            internal: err.to_string(),
        })?;

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
