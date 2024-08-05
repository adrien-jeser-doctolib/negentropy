use super::parser::Parser;
use crate::Key;

pub struct KeyWithParser<'a, KEY, PARSER>
where
    KEY: Key,
    PARSER: Parser,
{
    key: &'a KEY,
    parser: &'a PARSER,
}

impl<'a, KEY, PARSER> KeyWithParser<'a, KEY, PARSER>
where
    KEY: Key,
    PARSER: Parser,
{
    #[inline]
    pub const fn new(key: &'a KEY, parser: &'a PARSER) -> Self {
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
