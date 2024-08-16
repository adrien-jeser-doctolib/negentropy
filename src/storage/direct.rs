use super::parser::Parser;

pub trait DKey {
    fn name(&self) -> String;
}

pub struct DKeyWithParser<'a, DKEY, PARSER>
where
    DKEY: DKey,
    PARSER: Parser,
{
    key: &'a DKEY,
    parser: &'a PARSER,
}

impl<'a, DKEY, PARSER> DKeyWithParser<'a, DKEY, PARSER>
where
    DKEY: DKey,
    PARSER: Parser,
{
    #[inline]
    pub const fn new(key: &'a DKEY, parser: &'a PARSER) -> Self {
        Self { key, parser }
    }

    #[inline]
    pub const fn key(&self) -> &DKEY {
        self.key
    }

    #[inline]
    pub const fn parser(&self) -> &PARSER {
        self.parser
    }
}
