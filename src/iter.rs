use serde::Serialize;

use crate::{Result, Writer};

/// An iterable CSV creator
///
/// # Example
///
/// ```
/// use std::error::Error;
/// use csv_stream::WriterBuilder;
/// use serde::Serialize;
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<(), Box<dyn Error>> {
///     #[derive(Serialize)]
///     struct Row { foo: usize, bar: usize }
///     let rows = [
///         Row{ foo: 1, bar: 2 },
///         Row{ foo: 3, bar: 4 },
///     ];
///
///     let mut csv_iter = WriterBuilder::default().build_iter(rows);
///
///     let mut buf = vec![];
///     for row in csv_iter {
///         let row = row.unwrap();
///         buf.extend_from_slice(&row);
///     }
///
///     let data = String::from_utf8(buf)?;
///     assert_eq!(data, "foo,bar\n1,2\n3,4\n");
///     Ok(())
/// }
/// ```
pub struct Iter<I> {
    iter: I,

    writer: Writer,
}

impl<I: Iterator> Iter<I> {
    pub fn new(iter: impl IntoIterator<IntoIter = I>, writer: Writer) -> Self {
        Self {
            iter: iter.into_iter(),
            writer,
        }
    }
}

impl<I: Iterator> Iterator for Iter<I>
where
    I::Item: Serialize,
{
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let s = self.iter.next()?;
        let mut buf = vec![];
        Some(self.writer.serialize(&mut buf, s).map(|_| buf))
    }
}

#[cfg(test)]
mod tests {
    use crate::{Terminator, WriterBuilder};
    use serde::Serialize;

    use super::Iter;

    #[derive(Serialize)]
    struct Row<'a> {
        city: &'a str,
        country: &'a str,
        // Serde allows us to name our headers exactly,
        // even if they don't match our struct field names.
        #[serde(rename = "popcount")]
        population: u64,
    }

    const ROWS: [Row<'static>; 2] = [
        Row {
            city: "Boston",
            country: "United States",
            population: 4628910,
        },
        Row {
            city: "Concord",
            country: "United States",
            population: 42695,
        },
    ];

    #[test]
    fn serialize() {

        let writer = WriterBuilder::default().build();

        let i = Iter::new(ROWS, writer);

        let buf = i
            .map(Result::unwrap)
            .flatten()
            .collect();

        let buf = String::from_utf8(buf).unwrap();

        assert_eq!(
            buf,
            r#"city,country,popcount
Boston,United States,4628910
Concord,United States,42695
"#
        )
    }

    #[test]
    fn config() {
        let writer = WriterBuilder::default()
            .has_headers(false)
            .delimiter(b';')
            .terminator(Terminator::CRLF)
            .build();

        let i = Iter::new(ROWS, writer);

        let buf = i
            .map(Result::unwrap)
            .flatten()
            .collect();

        let buf = String::from_utf8(buf).unwrap();

        assert_eq!(
            buf,
            r#"Boston;United States;4628910
Concord;United States;42695
"#.replace("\n", "\r\n")
        )
    }
}
