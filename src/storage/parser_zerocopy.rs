use rkyv::ser::serializers::{
    AlignedSerializer, AllocScratch, AllocSerializer, CompositeSerializer, FallbackScratch,
    HeapScratch, SharedSerializeMap,
};
use rkyv::ser::Serializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{AlignedVec, Archive, CheckBytes, Deserialize, Infallible};

use super::ParserError;

pub trait SerializeZeroCopy = rkyv::Serialize<
    CompositeSerializer<
        AlignedSerializer<AlignedVec>,
        FallbackScratch<HeapScratch<0>, AllocScratch>,
        SharedSerializeMap,
    >,
>;

pub trait ParserZeroCopy {
    fn serialize_value<VALUE>(&self, value: &VALUE) -> Result<Vec<u8>, ParserError>
    where
        VALUE: SerializeZeroCopy;

    fn deserialize_value<'content, CONTENT>(
        &'content self,
        content: &'content [u8],
    ) -> Result<&'content CONTENT, ParserError>
    where
        CONTENT: Deserialize<CONTENT, Infallible> + Archive<Archived = CONTENT>,
        <CONTENT as Archive>::Archived: CheckBytes<DefaultValidator<'content>>;

    fn mime(&self) -> String;
}

#[derive(Default)]
pub struct Rkyv;

impl ParserZeroCopy for Rkyv {
    #[inline]
    fn serialize_value<VALUE>(&self, value: &VALUE) -> Result<Vec<u8>, ParserError>
    where
        VALUE: SerializeZeroCopy,
    {
        let mut serializer = AllocSerializer::<0>::default();
        serializer
            .serialize_value(value)
            .map_err(|err| ParserError::Serde {
                internal: err.to_string(),
            })?;
        let bytes = serializer.into_serializer().into_inner();
        Ok(bytes.to_vec())
    }

    #[inline]
    fn deserialize_value<'content, CONTENT>(
        &'content self,
        content: &'content [u8],
    ) -> Result<&'content CONTENT, ParserError>
    where
        CONTENT: Deserialize<CONTENT, Infallible> + Archive<Archived = CONTENT>,
        <CONTENT as Archive>::Archived: CheckBytes<DefaultValidator<'content>>,
    {
        let content_deserialized =
            rkyv::check_archived_root::<CONTENT>(content).map_err(|err| ParserError::Serde {
                internal: err.to_string(),
            })?;

        Ok(content_deserialized)
    }

    #[inline]
    fn mime(&self) -> String {
        "application/rkyv".to_owned()
    }
}
