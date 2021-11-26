use std::error::Error as StdError;
use std::fmt;
use std::result;

/// A type alias for `Result<T, csv_stream::Error>`.
pub type Result<T> = result::Result<T, Error>;

/// An error that can occur when processing CSV data.
///
/// This error can happen when writing or reading CSV data.
///
/// There are some important scenarios where an error is impossible to occur.
/// For example, if a CSV reader is used on an in-memory buffer with the
/// `flexible` option enabled and one is reading records as raw byte strings,
/// then no error can occur.
#[derive(Debug)]
pub struct Error(Box<ErrorKind>);

impl Error {
    /// A crate private constructor for `Error`.
    pub(crate) fn new(kind: ErrorKind) -> Error {
        Error(Box::new(kind))
    }

    /// Return the specific type of this error.
    pub fn kind(&self) -> &ErrorKind {
        &self.0
    }

    /// Unwrap this error into its underlying type.
    pub fn into_kind(self) -> ErrorKind {
        *self.0
    }
}

/// The specific type of an error.
#[derive(Debug)]
#[non_exhaustive]
pub enum ErrorKind {
    /// This error occurs when two records with an unequal number of fields
    /// are found. This error only occurs when the `flexible` option in a
    /// CSV reader/writer is disabled.
    UnequalLengths {
        /// The expected number of fields in a record. This is the number of
        /// fields in the record read prior to the record indicated by
        /// `pos`.
        expected_len: u64,
        /// The number of fields in the bad record.
        len: u64,
    },
    /// An error of this kind occurs only when using the Serde serializer.
    Serialize(String),
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match *self.0 {
            ErrorKind::UnequalLengths { .. } => None,
            ErrorKind::Serialize(_) => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self.0 {
            ErrorKind::UnequalLengths {
                expected_len,
                len,
            } => {
                write!(
                    f,
                    "CSV error: \
                     found record with {} fields, but the previous record \
                     has {} fields",
                    len, expected_len
                )
            }
            ErrorKind::Serialize(ref err) => {
                write!(f, "CSV write error: {}", err)
            }
        }
    }
}
