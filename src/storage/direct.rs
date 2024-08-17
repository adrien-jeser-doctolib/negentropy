use super::parser::ParserCopy;

pub trait DKey {
    fn name(&self) -> String;
}

pub struct DKeyWithParserCopy<'a, DKEY, PARSER>
where
    DKEY: DKey,
    PARSER: ParserCopy,
{
    key: &'a DKEY,
    parser: &'a PARSER,
}

impl<'a, DKEY, PARSER> DKeyWithParserCopy<'a, DKEY, PARSER>
where
    DKEY: DKey,
    PARSER: ParserCopy,
{
    #[inline]
    pub const fn new(key: &'a DKEY, parser: &'a PARSER) -> Self {
        Self { key, parser }
    }

    #[inline]
    #[must_use]
    pub const fn key(&self) -> &DKEY {
        self.key
    }

    #[inline]
    #[must_use]
    pub const fn parser(&self) -> &PARSER {
        self.parser
    }
}
