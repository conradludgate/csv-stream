use pin_project::pin_project;
use serde::Serialize;

use crate::{Result, Writer};

/// A Streamable CSV creator
///
/// # Example
///
/// ```
/// use std::error::Error;
/// use csv_stream::WriterBuilder;
/// use serde::Serialize;
/// use futures::StreamExt;
///
/// # #[tokio::main]
/// # async fn main() { example().await.unwrap(); }
/// async fn example() -> Result<(), Box<dyn Error>> {
///     #[derive(Serialize)]
///     struct Row { foo: usize, bar: usize }
///     let rows = [
///         Row{ foo: 1, bar: 2 },
///         Row{ foo: 3, bar: 4 },
///     ];
///     // a Stream over rows
///     let stream = futures::stream::iter(rows);
///
///     let mut csv_stream = WriterBuilder::default().build_stream(stream);
///
///     let mut buf = vec![];
///     while let Some(row) = csv_stream.next().await {
///         let row = row.unwrap();
///         buf.extend_from_slice(&row);
///     }
///
///     let data = String::from_utf8(buf)?;
///     assert_eq!(data, "foo,bar\n1,2\n3,4\n");
///     Ok(())
/// }
/// ```
#[pin_project]
pub struct Stream<S> {
    #[pin]
    stream: S,

    writer: Writer,
}
impl<S> Stream<S> {
    pub fn new(stream: S, writer: Writer) -> Self {
        Self { stream, writer }
    }
}

impl<S: futures::Stream> futures::Stream for Stream<S>
where
    S::Item: Serialize,
{
    type Item = Result<Vec<u8>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let p = self.project();
        let s = match p.stream.poll_next(cx) {
            std::task::Poll::Pending => return std::task::Poll::Pending,
            std::task::Poll::Ready(None) => return std::task::Poll::Ready(None),
            std::task::Poll::Ready(Some(s)) => s,
        };

        let mut buf = vec![];
        p.writer.serialize(&mut buf, s)?;
        std::task::Poll::Ready(Some(Ok(buf)))
    }
}

#[cfg(test)]
mod tests {
    use crate::{Terminator, WriterBuilder};
    use serde::Serialize;

    use super::Stream;
    use futures::StreamExt;

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

    #[tokio::test]
    async fn serialize() {

        let writer = WriterBuilder::default().build();

        let row_stream = futures::stream::iter(ROWS);
        let csv_stream = Stream::new(row_stream, writer);

        let buf = csv_stream
            .map(Result::unwrap)
            .map(futures::stream::iter)
            .flatten()
            .collect()
            .await;

        let buf = String::from_utf8(buf).unwrap();

        assert_eq!(
            buf,
            r#"city,country,popcount
Boston,United States,4628910
Concord,United States,42695
"#
        )
    }

    #[tokio::test]
    async fn config() {
        let writer = WriterBuilder::default()
            .has_headers(false)
            .delimiter(b';')
            .terminator(Terminator::CRLF)
            .build();

        let row_stream = futures::stream::iter(ROWS);
        let csv_stream = Stream::new(row_stream, writer);

        let buf = csv_stream
            .map(Result::unwrap)
            .map(futures::stream::iter)
            .flatten()
            .collect()
            .await;

        let buf = String::from_utf8(buf).unwrap();

        assert_eq!(
            buf,
            r#"Boston;United States;4628910
Concord;United States;42695
"#.replace("\n", "\r\n")
        )
    }
}
