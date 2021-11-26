mod error;
mod iter;
mod serializer;
#[cfg(feature = "stream")]
mod stream;
mod writer;

pub use error::{Error, ErrorKind, Result};
pub use iter::Iter;
#[cfg(feature = "stream")]
pub use stream::Stream;
pub use writer::{Writer, WriterBuilder};

/// The quoting style to use when writing CSV data.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum QuoteStyle {
    /// This puts quotes around every field. Always.
    Always,
    /// This puts quotes around fields only when necessary.
    ///
    /// They are necessary when fields contain a quote, delimiter or record
    /// terminator. Quotes are also necessary when writing an empty record
    /// (which is indistinguishable from a record with one empty field).
    ///
    /// This is the default.
    Necessary,
    /// This puts quotes around all fields that are non-numeric. Namely, when
    /// writing a field that does not parse as a valid float or integer, then
    /// quotes will be used even if they aren't strictly necessary.
    NonNumeric,
    /// This *never* writes quotes, even if it would produce invalid CSV data.
    Never,
}

impl QuoteStyle {
    fn to_core(self) -> csv_core::QuoteStyle {
        match self {
            QuoteStyle::Always => csv_core::QuoteStyle::Always,
            QuoteStyle::Necessary => csv_core::QuoteStyle::Necessary,
            QuoteStyle::NonNumeric => csv_core::QuoteStyle::NonNumeric,
            QuoteStyle::Never => csv_core::QuoteStyle::Never,
        }
    }
}

impl Default for QuoteStyle {
    fn default() -> QuoteStyle {
        QuoteStyle::Necessary
    }
}

/// A record terminator.
///
/// Use this to specify the record terminator while parsing CSV. The default is
/// CRLF, which treats `\r`, `\n` or `\r\n` as a single record terminator.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]

pub enum Terminator {
    /// Parses `\r`, `\n` or `\r\n` as a single record terminator.
    CRLF,
    /// Parses the byte given as a record terminator.
    Any(u8),
}

impl Terminator {
    /// Convert this to the csv_core type of the same name.
    fn to_core(self) -> csv_core::Terminator {
        match self {
            Terminator::CRLF => csv_core::Terminator::CRLF,
            Terminator::Any(b) => csv_core::Terminator::Any(b),
        }
    }
}

impl Default for Terminator {
    fn default() -> Terminator {
        Terminator::CRLF
    }
}

/// The whitespace preservation behaviour when reading CSV data.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]

pub enum Trim {
    /// Preserves fields and headers. This is the default.
    None,
    /// Trim whitespace from headers.
    Headers,
    /// Trim whitespace from fields, but not headers.
    Fields,
    /// Trim whitespace from fields and headers.
    All,
}

impl Default for Trim {
    fn default() -> Trim {
        Trim::None
    }
}
