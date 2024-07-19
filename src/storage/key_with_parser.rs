use super::parser::Parser;
use crate::Key;

pub struct KeyWithParser<KEY, PARSER>
where
    KEY: Key,
    PARSER: Parser,
{
    key: KEY,
    parser: PARSER,
}

impl<KEY, PARSER> KeyWithParser<KEY, PARSER>
where
    KEY: Key,
    PARSER: Parser,
{
    #[inline]
    pub const fn new(key: KEY, parser: PARSER) -> Self {
        Self { key, parser }
    }

    #[inline]
    pub const fn key(&self) -> &KEY {
        &self.key
    }

    #[inline]
    pub const fn parser(&self) -> &PARSER {
        &self.parser
    }
}
