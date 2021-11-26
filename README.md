# csv-stream

For building CSVs as Streams or Iterators.

```rust
#[derive(Serialize)]
struct Row<'a> {
    city: &'a str,
    country: &'a str,
    // Serde allows us to name our headers exactly,
    // even if they don't match our struct field names.
    #[serde(rename = "popcount")]
    population: u64,
}

let rows = vec![
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

let writer = WriterBuilder::default().build();

let row_stream = futures::stream::iter(ROWS);
let csv_stream = Stream::new(row_stream, writer);

let mut buf = vec![];
while let Some(res) = csv_stream.next().await {
    buf.extend_from_slice(&res.unwrap())
}

let buf = String::from_utf8(buf).unwrap();

assert_eq!(
    buf,
    r#"city,country,popcount
Boston,United States,4628910
Concord,United States,42695
"#
);
```
