use csv_core::{self, WriteResult, Writer as CoreWriter, WriterBuilder as CoreWriterBuilder};
use serde::Serialize;

use crate::error::{Error, ErrorKind, Result};
use crate::serializer::{serialize, serialize_header};
use crate::{QuoteStyle, Terminator};

/// Builds a CSV writer with various configuration knobs.
///
/// This builder can be used to tweak the field delimiter, record terminator
/// and more. Once a CSV `Writer` is built, its configuration cannot be
/// changed.
#[derive(Debug)]
pub struct WriterBuilder {
    builder: CoreWriterBuilder,
    capacity: usize,
    flexible: bool,
    has_headers: bool,
}

impl Default for WriterBuilder {
    fn default() -> WriterBuilder {
        WriterBuilder {
            builder: CoreWriterBuilder::default(),
            capacity: 8 * (1 << 10),
            flexible: false,
            has_headers: true,
        }
    }
}

impl WriterBuilder {
    pub fn build(&self) -> Writer {
        Writer::new(self)
    }

    /// The field delimiter to use when writing CSV.
    ///
    /// The default is `b','`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_stream::WriterBuilder;
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default()
    ///         .delimiter(b';')
    ///         .build();
    ///
    ///     let mut buf = vec![];
    ///     wtr.write_record(&mut buf, &["a", "b", "c"])?;
    ///     wtr.write_record(&mut buf, &["x", "y", "z"])?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "a;b;c\nx;y;z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn delimiter(&mut self, delimiter: u8) -> &mut WriterBuilder {
        self.builder.delimiter(delimiter);
        self
    }

    /// Whether to write a header row before writing any other row.
    ///
    /// When this is enabled and the `serialize` method is used to write data
    /// with something that contains field names (i.e., a struct), then a
    /// header row is written containing the field names before any other row
    /// is written.
    ///
    /// This option has no effect when using other methods to write rows. That
    /// is, if you don't use `serialize`, then you must write your header row
    /// explicitly if you want a header row.
    ///
    /// This is enabled by default.
    ///
    /// # Example: with headers
    ///
    /// This shows how the header will be automatically written from the field
    /// names of a struct.
    ///
    /// ```
    /// use std::error::Error;
    ///
    /// use csv_stream::WriterBuilder;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Row<'a> {
    ///     city: &'a str,
    ///     country: &'a str,
    ///     // Serde allows us to name our headers exactly,
    ///     // even if they don't match our struct field names.
    ///     #[serde(rename = "popcount")]
    ///     population: u64,
    /// }
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default().build();
    ///
    ///     let mut buf = vec![];
    ///     wtr.serialize(
    ///         &mut buf,
    ///         Row {
    ///             city: "Boston",
    ///             country: "United States",
    ///             population: 4628910,
    ///         },
    ///     )?;
    ///     wtr.serialize(
    ///         &mut buf,
    ///         Row {
    ///             city: "Concord",
    ///             country: "United States",
    ///             population: 42695,
    ///         },
    ///     )?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "\
    /// city,country,popcount
    /// Boston,United States,4628910
    /// Concord,United States,42695
    /// ");
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Example: without headers
    ///
    /// This shows that serializing things that aren't structs (in this case,
    /// a tuple struct) won't result in a header row being written. This means
    /// you usually don't need to set `has_headers(false)` unless you
    /// explicitly want to both write custom headers and serialize structs.
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_stream::WriterBuilder;
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default().build();
    ///     let mut buf = vec![];
    ///     wtr.serialize(&mut buf, ("Boston", "United States", 4628910))?;
    ///     wtr.serialize(&mut buf, ("Concord", "United States", 42695))?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "\
    /// Boston,United States,4628910
    /// Concord,United States,42695
    /// ");
    ///     Ok(())
    /// }
    /// ```
    pub fn has_headers(&mut self, yes: bool) -> &mut WriterBuilder {
        self.has_headers = yes;
        self
    }

    /// Whether the number of fields in records is allowed to change or not.
    ///
    /// When disabled (which is the default), writing CSV data will return an
    /// error if a record is written with a number of fields different from the
    /// number of fields written in a previous record.
    ///
    /// When enabled, this error checking is turned off.
    ///
    /// # Example: writing flexible records
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_stream::WriterBuilder;
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default()
    ///         .flexible(true)
    ///         .build();
    ///     let mut buf = vec![];
    ///     wtr.write_record(&mut buf, &["a", "b"])?;
    ///     wtr.write_record(&mut buf, &["x", "y", "z"])?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "a,b\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Example: error when `flexible` is disabled
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_stream::WriterBuilder;
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default()
    ///         .flexible(false)
    ///         .build();
    ///     let mut buf = vec![];
    ///     wtr.write_record(&mut buf, &["a", "b"])?;
    ///     let err = wtr.write_record(&mut buf, &["x", "y", "z"]).unwrap_err();
    ///     match *err.kind() {
    ///         csv_stream::ErrorKind::UnequalLengths { expected_len, len, .. } => {
    ///             assert_eq!(expected_len, 2);
    ///             assert_eq!(len, 3);
    ///         }
    ///         ref wrong => {
    ///             panic!("expected UnequalLengths but got {:?}", wrong);
    ///         }
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn flexible(&mut self, yes: bool) -> &mut WriterBuilder {
        self.flexible = yes;
        self
    }

    /// The record terminator to use when writing CSV.
    ///
    /// A record terminator can be any single byte. The default is `\n`.
    ///
    /// Note that RFC 4180 specifies that record terminators should be `\r\n`.
    /// To use `\r\n`, use the special `Terminator::CRLF` value.
    ///
    /// # Example: CRLF
    ///
    /// This shows how to use RFC 4180 compliant record terminators.
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_stream::{Terminator, WriterBuilder};
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default()
    ///         .terminator(Terminator::CRLF)
    ///         .build();
    ///     let mut buf = vec![];
    ///     wtr.write_record(&mut buf, &["a", "b", "c"])?;
    ///     wtr.write_record(&mut buf, &["x", "y", "z"])?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "a,b,c\r\nx,y,z\r\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn terminator(&mut self, term: Terminator) -> &mut WriterBuilder {
        self.builder.terminator(term.to_core());
        self
    }

    /// The quoting style to use when writing CSV.
    ///
    /// By default, this is set to `QuoteStyle::Necessary`, which will only
    /// use quotes when they are necessary to preserve the integrity of data.
    ///
    /// Note that unless the quote style is set to `Never`, an empty field is
    /// quoted if it is the only field in a record.
    ///
    /// # Example: non-numeric quoting
    ///
    /// This shows how to quote non-numeric fields only.
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_stream::{QuoteStyle, WriterBuilder};
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default()
    ///         .quote_style(QuoteStyle::NonNumeric)
    ///         .build();
    ///     let mut buf = vec![];
    ///     wtr.write_record(&mut buf, &["a", "5", "c"])?;
    ///     wtr.write_record(&mut buf, &["3.14", "y", "z"])?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "\"a\",5,\"c\"\n3.14,\"y\",\"z\"\n");
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Example: never quote
    ///
    /// This shows how the CSV writer can be made to never write quotes, even
    /// if it sacrifices the integrity of the data.
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_stream::{QuoteStyle, WriterBuilder};
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default()
    ///         .quote_style(QuoteStyle::Never)
    ///         .build();
    ///     let mut buf = vec![];
    ///     wtr.write_record(&mut buf, &["a", "foo\nbar", "c"])?;
    ///     wtr.write_record(&mut buf, &["g\"h\"i", "y", "z"])?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "a,foo\nbar,c\ng\"h\"i,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn quote_style(&mut self, style: QuoteStyle) -> &mut WriterBuilder {
        self.builder.quote_style(style.to_core());
        self
    }

    /// The quote character to use when writing CSV.
    ///
    /// The default is `b'"'`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_stream::WriterBuilder;
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default()
    ///         .quote(b'\'')
    ///         .build();
    ///     let mut buf = vec![];
    ///     wtr.write_record(&mut buf, &["a", "foo\nbar", "c"])?;
    ///     wtr.write_record(&mut buf, &["g'h'i", "y\"y\"y", "z"])?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "a,'foo\nbar',c\n'g''h''i',y\"y\"y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn quote(&mut self, quote: u8) -> &mut WriterBuilder {
        self.builder.quote(quote);
        self
    }

    /// Enable double quote escapes.
    ///
    /// This is enabled by default, but it may be disabled. When disabled,
    /// quotes in field data are escaped instead of doubled.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_stream::WriterBuilder;
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default()
    ///         .double_quote(false)
    ///         .build();
    ///     let mut buf = vec![];
    ///     wtr.write_record(&mut buf, &["a", "foo\"bar", "c"])?;
    ///     wtr.write_record(&mut buf, &["x", "y", "z"])?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "a,\"foo\\\"bar\",c\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn double_quote(&mut self, yes: bool) -> &mut WriterBuilder {
        self.builder.double_quote(yes);
        self
    }

    /// The escape character to use when writing CSV.
    ///
    /// In some variants of CSV, quotes are escaped using a special escape
    /// character like `\` (instead of escaping quotes by doubling them).
    ///
    /// By default, writing these idiosyncratic escapes is disabled, and is
    /// only used when `double_quote` is disabled.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_stream::WriterBuilder;
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default()
    ///         .double_quote(false)
    ///         .escape(b'$')
    ///         .build();
    ///     let mut buf = vec![];
    ///     wtr.write_record(&mut buf, &["a", "foo\"bar", "c"])?;
    ///     wtr.write_record(&mut buf, &["x", "y", "z"])?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "a,\"foo$\"bar\",c\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn escape(&mut self, escape: u8) -> &mut WriterBuilder {
        self.builder.escape(escape);
        self
    }
}

/// A already configured CSV writer.
///
/// A CSV writer takes as input Rust values and writes those values in a valid
/// CSV format as output.
///
/// While CSV writing is considerably easier than parsing CSV, a proper writer
/// will do a number of things for you:
///
/// 1. Quote fields when necessary.
/// 2. Check that all records have the same number of fields.
/// 3. Write records with a single empty field correctly.
/// 4. Automatically serialize normal Rust types to CSV records. When that
///    type is a struct, a header row is automatically written corresponding
///    to the fields of that struct.
/// 5. Use buffering intelligently and otherwise avoid allocation. (This means
///    that callers should not do their own buffering.)
///
/// All of the above can be configured using a
/// [`WriterBuilder`](struct.WriterBuilder.html).
///
/// Note that the default configuration of a `Writer` uses `\n` for record
/// terminators instead of `\r\n` as specified by RFC 4180. Use the
/// `terminator` method on `WriterBuilder` to set the terminator to `\r\n` if
/// it's desired.
#[derive(Debug)]
pub struct Writer {
    core: CoreWriter,
    state: WriterState,
}

#[derive(Debug)]
struct WriterState {
    /// Whether the Serde serializer should attempt to write a header row.
    header: HeaderState,
    /// Whether inconsistent record lengths are allowed.
    flexible: bool,
    /// The number of fields writtein in the first record. This is compared
    /// with `fields_written` on all subsequent records to check for
    /// inconsistent record lengths.
    first_field_count: Option<u64>,
    /// The number of fields written in this record. This is used to report
    /// errors for inconsistent record lengths if `flexible` is disabled.
    fields_written: u64,
    /// This is set immediately before flushing the buffer and then unset
    /// immediately after flushing the buffer. This avoids flushing the buffer
    /// twice if the inner writer panics.
    panicked: bool,
}

/// HeaderState encodes a small state machine for handling header writes.
#[derive(Debug)]
enum HeaderState {
    /// Indicates that we should attempt to write a header.
    Write,
    /// Indicates that writing a header was attempt, and a header was written.
    DidWrite,
    /// Indicates that writing a header was attempted, but no headers were
    /// written or the attempt failed.
    DidNotWrite,
    /// This state is used when headers are disabled. It cannot transition
    /// to any other state.
    None,
}

impl Default for Writer {
    fn default() -> Self {
        WriterBuilder::default().build()
    }
}

impl Writer {
    fn new(builder: &WriterBuilder) -> Writer {
        let header_state = if builder.has_headers {
            HeaderState::Write
        } else {
            HeaderState::None
        };
        Writer {
            core: builder.builder.build(),
            state: WriterState {
                header: header_state,
                flexible: builder.flexible,
                first_field_count: None,
                fields_written: 0,
                panicked: false,
            },
        }
    }

    /// Serialize a single record using Serde.
    ///
    /// # Example
    ///
    /// This shows how to serialize normal Rust structs as CSV records. The
    /// fields of the struct are used to write a header row automatically.
    /// (Writing the header row automatically can be disabled by building the
    /// CSV writer with a [`WriterBuilder`](struct.WriterBuilder.html) and
    /// calling the `has_headers` method.)
    ///
    /// ```
    /// use std::error::Error;
    ///
    /// use csv_stream::WriterBuilder;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Row<'a> {
    ///     city: &'a str,
    ///     country: &'a str,
    ///     // Serde allows us to name our headers exactly,
    ///     // even if they don't match our struct field names.
    ///     #[serde(rename = "popcount")]
    ///     population: u64,
    /// }
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default().build();
    ///     let mut buf = vec![];
    ///     wtr.serialize(
    ///         &mut buf,
    ///         Row {
    ///             city: "Boston",
    ///             country: "United States",
    ///             population: 4628910,
    ///         },
    ///     )?;
    ///     wtr.serialize(
    ///         &mut buf,
    ///         Row {
    ///             city: "Concord",
    ///             country: "United States",
    ///             population: 42695,
    ///         },
    ///     )?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "\
    /// city,country,popcount
    /// Boston,United States,4628910
    /// Concord,United States,42695
    /// ");
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Rules
    ///
    /// The behavior of `serialize` is fairly simple:
    ///
    /// 1. Nested containers (tuples, `Vec`s, structs, etc.) are always
    ///    flattened (depth-first order).
    ///
    /// 2. If `has_headers` is `true` and the type contains field names, then
    ///    a header row is automatically generated.
    ///
    /// However, some container types cannot be serialized, and if
    /// `has_headers` is `true`, there are some additional restrictions on the
    /// types that can be serialized. See below for details.
    ///
    /// For the purpose of this section, Rust types can be divided into three
    /// categories: scalars, non-struct containers, and structs.
    ///
    /// ## Scalars
    ///
    /// Single values with no field names are written like the following. Note
    /// that some of the outputs may be quoted, according to the selected
    /// quoting style.
    ///
    /// | Name | Example Type | Example Value | Output |
    /// | ---- | ---- | ---- | ---- |
    /// | boolean | `bool` | `true` | `true` |
    /// | integers | `i8`, `i16`, `i32`, `i64`, `i128`, `u8`, `u16`, `u32`, `u64`, `u128` | `5` | `5` |
    /// | floats | `f32`, `f64` | `3.14` | `3.14` |
    /// | character | `char` | `'☃'` | `☃` |
    /// | string | `&str` | `"hi"` | `hi` |
    /// | bytes | `&[u8]` | `b"hi"[..]` | `hi` |
    /// | option | `Option` | `None` | *empty* |
    /// | option |          | `Some(5)` | `5` |
    /// | unit | `()` | `()` | *empty* |
    /// | unit struct | `struct Foo;` | `Foo` | `Foo` |
    /// | unit enum variant | `enum E { A, B }` | `E::A` | `A` |
    /// | newtype struct | `struct Foo(u8);` | `Foo(5)` | `5` |
    /// | newtype enum variant | `enum E { A(u8) }` | `E::A(5)` | `5` |
    ///
    /// Note that this table includes simple structs and enums. For example, to
    /// serialize a field from either an integer or a float type, one can do
    /// this:
    ///
    /// ```
    /// use std::error::Error;
    ///
    /// use csv_stream::WriterBuilder;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Row {
    ///     label: String,
    ///     value: Value,
    /// }
    ///
    /// #[derive(Serialize)]
    /// enum Value {
    ///     Integer(i64),
    ///     Float(f64),
    /// }
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default().build();
    ///     let mut buf = vec![];
    ///     wtr.serialize(
    ///         &mut buf,
    ///         Row {
    ///             label: "foo".to_string(),
    ///             value: Value::Integer(3),
    ///         },
    ///     )?;
    ///     wtr.serialize(
    ///         &mut buf,
    ///         Row {
    ///             label: "bar".to_string(),
    ///             value: Value::Float(3.14),
    ///         },
    ///     )?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "\
    /// label,value
    /// foo,3
    /// bar,3.14
    /// ");
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Non-Struct Containers
    ///
    /// Nested containers are flattened to their scalar components, with the
    /// exception of a few types that are not allowed:
    ///
    /// | Name | Example Type | Example Value | Output |
    /// | ---- | ---- | ---- | ---- |
    /// | sequence | `Vec<u8>` | `vec![1, 2, 3]` | `1,2,3` |
    /// | tuple | `(u8, bool)` | `(5, true)` | `5,true` |
    /// | tuple struct | `Foo(u8, bool)` | `Foo(5, true)` | `5,true` |
    /// | tuple enum variant | `enum E { A(u8, bool) }` | `E::A(5, true)` | *error* |
    /// | struct enum variant | `enum E { V { a: u8, b: bool } }` | `E::V { a: 5, b: true }` | *error* |
    /// | map | `BTreeMap<K, V>` | `BTreeMap::new()` | *error* |
    ///
    /// ## Structs
    ///
    /// Like the other containers, structs are flattened to their scalar
    /// components:
    ///
    /// | Name | Example Type | Example Value | Output |
    /// | ---- | ---- | ---- | ---- |
    /// | struct | `struct Foo { a: u8, b: bool }` | `Foo { a: 5, b: true }` | `5,true` |
    ///
    /// If `has_headers` is `false`, then there are no additional restrictions;
    /// types can be nested arbitrarily. For example:
    ///
    /// ```
    /// use std::error::Error;
    ///
    /// use csv_stream::WriterBuilder;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Row {
    ///     label: String,
    ///     values: Vec<f64>,
    /// }
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default()
    ///         .has_headers(false)
    ///         .build();
    ///
    ///     let mut buf = vec![];
    ///     wtr.serialize(
    ///         &mut buf,
    ///         Row {
    ///             label: "foo".to_string(),
    ///             values: vec![1.1234, 2.5678, 3.14],
    ///         },
    ///     )?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "\
    /// foo,1.1234,2.5678,3.14
    /// ");
    ///     Ok(())
    /// }
    /// ```
    ///
    /// However, if `has_headers` were enabled in the above example, then
    /// serialization would return an error. Specifically, when `has_headers` is
    /// `true`, there are two restrictions:
    ///
    /// 1. Named field values in structs must be scalars.
    ///
    /// 2. All scalars must be named field values in structs.
    ///
    /// Other than these two restrictions, types can be nested arbitrarily.
    /// Here are a few examples:
    ///
    /// | Value | Header | Record |
    /// | ---- | ---- | ---- |
    /// | `(Foo { x: 5, y: 6 }, Bar { z: true })` | `x,y,z` | `5,6,true` |
    /// | `vec![Foo { x: 5, y: 6 }, Foo { x: 7, y: 8 }]` | `x,y,x,y` | `5,6,7,8` |
    /// | `(Foo { x: 5, y: 6 }, vec![Bar { z: Baz(true) }])` | `x,y,z` | `5,6,true` |
    /// | `Foo { x: 5, y: (6, 7) }` | *error: restriction 1* | `5,6,7` |
    /// | `(5, Foo { x: 6, y: 7 }` | *error: restriction 2* | `5,6,7` |
    /// | `(Foo { x: 5, y: 6 }, true)` | *error: restriction 2* | `5,6,true` |
    pub fn serialize<S: Serialize>(&mut self, buf: &mut Vec<u8>, record: S) -> Result<()> {
        if let HeaderState::Write = self.state.header {
            let wrote_header = serialize_header(self, buf, &record)?;
            if wrote_header {
                self.write_terminator(buf)?;
                self.state.header = HeaderState::DidWrite;
            } else {
                self.state.header = HeaderState::DidNotWrite;
            };
        }
        serialize(self, buf, &record)?;
        self.write_terminator(buf)?;
        Ok(())
    }

    /// Write a single record.
    ///
    /// This method accepts something that can be turned into an iterator that
    /// yields elements that can be represented by a `&[u8]`.
    ///
    /// This may be called with an empty iterator, which will cause a record
    /// terminator to be written. If no fields had been written, then a single
    /// empty field is written before the terminator.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_stream::WriterBuilder;
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default().build();
    ///     let mut buf = vec![];
    ///     wtr.write_record(&mut buf, &["a", "b", "c"])?;
    ///     wtr.write_record(&mut buf, &["x", "y", "z"])?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "a,b,c\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn write_record<I, T>(&mut self, buf: &mut Vec<u8>, record: I) -> Result<()>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        for field in record.into_iter() {
            self.write_field_impl(buf, field)?;
        }
        self.write_terminator(buf)
    }

    /// Write a single field.
    ///
    /// One should prefer using `write_record` over this method. It is provided
    /// for cases where writing a field at a time is more convenient than
    /// writing a record at a time.
    ///
    /// Note that if this API is used, `write_record` should be called with an
    /// empty iterator to write a record terminator.
    ///
    /// # Example
    ///
    /// ```
    /// use std::error::Error;
    /// use csv_stream::WriterBuilder;
    ///
    /// # fn main() { example().unwrap(); }
    /// fn example() -> Result<(), Box<dyn Error>> {
    ///     let mut wtr = WriterBuilder::default().build();
    ///     let mut buf = vec![];
    ///     wtr.write_field(&mut buf, "a")?;
    ///     wtr.write_field(&mut buf, "b")?;
    ///     wtr.write_field(&mut buf, "c")?;
    ///     wtr.write_record(&mut buf, None::<&[u8]>)?;
    ///     wtr.write_field(&mut buf, "x")?;
    ///     wtr.write_field(&mut buf, "y")?;
    ///     wtr.write_field(&mut buf, "z")?;
    ///     wtr.write_record(&mut buf, None::<&[u8]>)?;
    ///
    ///     let data = String::from_utf8(buf)?;
    ///     assert_eq!(data, "a,b,c\nx,y,z\n");
    ///     Ok(())
    /// }
    /// ```
    pub fn write_field<T: AsRef<[u8]>>(&mut self, buf: &mut Vec<u8>, field: T) -> Result<()> {
        self.write_field_impl(buf, field)
    }

    /// Implementation of write_field.
    ///
    /// This is a separate method so we can force the compiler to inline it
    /// into write_record.
    #[inline(always)]
    fn write_field_impl<T: AsRef<[u8]>>(&mut self, buf: &mut Vec<u8>, field: T) -> Result<()> {
        if self.state.fields_written > 0 {
            self.write_delimiter(buf)?;
        }
        let field = field.as_ref();

        extend(buf, 2 * field.len() + 2, |buf| {
            let (res, nin, nout) = self.core.field(field, buf);
            debug_assert_eq!(res, WriteResult::InputEmpty);
            debug_assert_eq!(nin, field.len());
            self.state.fields_written += 1;
            nout
        });

        Ok(())
    }

    /// Write a CSV delimiter.
    fn write_delimiter(&mut self, buf: &mut Vec<u8>) -> Result<()> {
        extend(buf, 2, |buf| {
            let (res, nout) = self.core.delimiter(buf);
            debug_assert_eq!(res, WriteResult::InputEmpty);
            nout
        });

        Ok(())
    }

    /// Write a CSV terminator.
    fn write_terminator(&mut self, buf: &mut Vec<u8>) -> Result<()> {
        self.check_field_count()?;
        extend(buf, 4, |buf| {
            let (res, nout) = self.core.terminator(buf);
            debug_assert_eq!(res, WriteResult::InputEmpty);
            self.state.fields_written = 0;
            nout
        });

        Ok(())
    }

    fn check_field_count(&mut self) -> Result<()> {
        if !self.state.flexible {
            match self.state.first_field_count {
                None => {
                    self.state.first_field_count = Some(self.state.fields_written);
                }
                Some(expected) if expected != self.state.fields_written => {
                    return Err(Error::new(ErrorKind::UnequalLengths {
                        expected_len: expected,
                        len: self.state.fields_written,
                    }))
                }
                Some(_) => {}
            }
        }
        Ok(())
    }
}

fn extend(buf: &mut Vec<u8>, max: usize, f: impl FnOnce(&mut [u8]) -> usize) {
    let len = buf.len();
    buf.resize(len + max, 0);
    let n = f(&mut buf[len..]);
    buf.resize(len + n, 0);
}

#[cfg(test)]
mod tests {
    use super::WriterBuilder;
    use serde::{serde_if_integer128, Serialize};

    fn buf_as_string(buf: Vec<u8>) -> String {
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn one_record() {
        let mut wtr = WriterBuilder::default().build();
        let mut buf = vec![];
        wtr.write_record(&mut buf, &["a", "b", "c"]).unwrap();

        assert_eq!(buf_as_string(buf), "a,b,c\n");
    }

    #[test]
    fn one_empty_record() {
        let mut wtr = WriterBuilder::default().build();
        let mut buf = vec![];
        wtr.write_record(&mut buf, &[""]).unwrap();

        assert_eq!(buf_as_string(buf), "\"\"\n");
    }

    #[test]
    fn two_empty_records() {
        let mut wtr = WriterBuilder::default().build();
        let mut buf = vec![];
        wtr.write_record(&mut buf, &[""]).unwrap();
        wtr.write_record(&mut buf, &[""]).unwrap();

        assert_eq!(buf_as_string(buf), "\"\"\n\"\"\n");
    }

    #[test]
    fn serialize_with_headers() {
        #[derive(Serialize)]
        struct Row {
            foo: i32,
            bar: f64,
            baz: bool,
        }

        let mut wtr = WriterBuilder::default().build();
        let mut buf = vec![];
        wtr.serialize(
            &mut buf,
            Row {
                foo: 42,
                bar: 42.5,
                baz: true,
            },
        )
        .unwrap();
        assert_eq!(buf_as_string(buf), "foo,bar,baz\n42,42.5,true\n");
    }

    #[test]
    fn serialize_no_headers() {
        #[derive(Serialize)]
        struct Row {
            foo: i32,
            bar: f64,
            baz: bool,
        }

        let mut wtr = WriterBuilder::default().has_headers(false).build();
        let mut buf = vec![];
        wtr.serialize(
            &mut buf,
            Row {
                foo: 42,
                bar: 42.5,
                baz: true,
            },
        )
        .unwrap();
        assert_eq!(buf_as_string(buf), "42,42.5,true\n");
    }

    serde_if_integer128! {
        #[test]
        fn serialize_no_headers_128() {
            #[derive(Serialize)]
            struct Row {
                foo: i128,
                bar: f64,
                baz: bool,
            }

            let mut wtr =
                WriterBuilder::default().has_headers(false).build();
            let mut buf = vec![];
            wtr.serialize(&mut buf, Row {
                foo: 9_223_372_036_854_775_808,
                bar: 42.5,
                baz: true,
            }).unwrap();
            assert_eq!(buf_as_string(buf), "9223372036854775808,42.5,true\n");
        }
    }

    #[test]
    fn serialize_tuple() {
        let mut wtr = WriterBuilder::default().build();
        let mut buf = vec![];
        wtr.serialize(&mut buf, (true, 1.3, "hi")).unwrap();
        assert_eq!(buf_as_string(buf), "true,1.3,hi\n");
    }
}
