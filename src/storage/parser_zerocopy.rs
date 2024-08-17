use rkyv::{
    ser::{
        serializers::{
            AlignedSerializer, AllocScratch, AllocSerializer, CompositeSerializer, FallbackScratch,
            HeapScratch, SharedSerializeMap,
        },
        Serializer,
    },
    validation::validators::DefaultValidator,
    AlignedVec, Archive, CheckBytes, Deserialize, Infallible,
};

use super::ParserError;
use crate::storage::ValueWhere;

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
        VALUE: ValueWhere + SerializeZeroCopy;

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
    fn serialize_value<VALUE>(&self, value: &VALUE) -> Result<Vec<u8>, ParserError>
    where
        VALUE: ValueWhere + SerializeZeroCopy,
    {
        let mut serializer = AllocSerializer::<0>::default();
        serializer.serialize_value(value).unwrap();
        let bytes = serializer.into_serializer().into_inner();
        Ok(bytes.to_vec())
    }

    fn deserialize_value<'content, CONTENT>(
        &'content self,
        content: &'content [u8],
    ) -> Result<&'content CONTENT, ParserError>
    where
        CONTENT: Deserialize<CONTENT, Infallible> + Archive<Archived = CONTENT>,
        <CONTENT as Archive>::Archived: CheckBytes<DefaultValidator<'content>>,
    {
        let content =
            rkyv::check_archived_root::<CONTENT>(content).map_err(|err| ParserError::Serde {
                internal: err.to_string(),
            })?;

        Ok(content)
    }

    fn mime(&self) -> String {
        todo!()
    }
}
