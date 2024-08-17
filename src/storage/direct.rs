use super::parser::ParserCopy;

pub trait DKey {
    fn name(&self) -> String;
}

pub struct DKeyWithParserCopy<'content, DKEY, PARSER>
where
    DKEY: DKey,
    PARSER: ParserCopy,
{
    key: &'content DKEY,
    parser: &'content PARSER,
}

impl<'content, DKEY, PARSER> DKeyWithParserCopy<'content, DKEY, PARSER>
where
    DKEY: DKey,
    PARSER: ParserCopy,
{
    #[inline]
    pub const fn new(key: &'content DKEY, parser: &'content PARSER) -> Self {
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
