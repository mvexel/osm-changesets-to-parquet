use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMillisecondBuilder, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use bzip2::read::MultiBzDecoder;
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use quick_xml::events::{BytesStart, Event};
use quick_xml::name::QName;
use quick_xml::Reader;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input OSM changeset XML file (.osm or .osm.bz2)
    #[arg(short, long)]
    input: String,

    /// Output Parquet file
    #[arg(short, long)]
    output: String,

    /// Batch size for writing records
    #[arg(short, long, default_value_t = 100000)]
    batch_size: usize,

    /// Continue processing on parse errors (saves what was successfully parsed)
    #[arg(long, default_value_t = false)]
    continue_on_error: bool,
}

#[derive(Debug, Default)]
struct Changeset {
    id: i64,
    created_at: Option<i64>,
    closed_at: Option<i64>,
    open: bool,
    user: Option<String>,
    uid: Option<i64>,
    min_lat: Option<f64>,
    min_lon: Option<f64>,
    max_lat: Option<f64>,
    max_lon: Option<f64>,
    num_changes: u32,
    comments_count: u32,
    description: Option<String>,
}

struct BatchBuilders {
    id: Int64Builder,
    created_at: TimestampMillisecondBuilder,
    closed_at: TimestampMillisecondBuilder,
    open: BooleanBuilder,
    user: StringBuilder,
    uid: Int64Builder,
    min_lat: Float64Builder,
    min_lon: Float64Builder,
    max_lat: Float64Builder,
    max_lon: Float64Builder,
    num_changes: UInt32Builder,
    comments_count: UInt32Builder,
    description: StringBuilder,
    len: usize,
}

impl BatchBuilders {
    fn with_capacity(capacity: usize) -> Self {
        // For string data we start with a modest byte capacity and Arrow will grow as needed.
        let string_byte_capacity = capacity * 16;

        Self {
            id: Int64Builder::with_capacity(capacity),
            created_at: TimestampMillisecondBuilder::with_capacity(capacity),
            closed_at: TimestampMillisecondBuilder::with_capacity(capacity),
            open: BooleanBuilder::with_capacity(capacity),
            user: StringBuilder::with_capacity(capacity, string_byte_capacity),
            uid: Int64Builder::with_capacity(capacity),
            min_lat: Float64Builder::with_capacity(capacity),
            min_lon: Float64Builder::with_capacity(capacity),
            max_lat: Float64Builder::with_capacity(capacity),
            max_lon: Float64Builder::with_capacity(capacity),
            num_changes: UInt32Builder::with_capacity(capacity),
            comments_count: UInt32Builder::with_capacity(capacity),
            description: StringBuilder::with_capacity(capacity, string_byte_capacity),
            len: 0,
        }
    }

    fn append(&mut self, cs: &Changeset) {
        self.id.append_value(cs.id);

        if let Some(ts) = cs.created_at {
            self.created_at.append_value(ts);
        } else {
            self.created_at.append_null();
        }

        if let Some(ts) = cs.closed_at {
            self.closed_at.append_value(ts);
        } else {
            self.closed_at.append_null();
        }

        self.open.append_value(cs.open);

        if let Some(ref user) = cs.user {
            self.user.append_value(user);
        } else {
            self.user.append_null();
        }

        if let Some(uid) = cs.uid {
            self.uid.append_value(uid);
        } else {
            self.uid.append_null();
        }

        if let Some(lat) = cs.min_lat {
            self.min_lat.append_value(lat);
        } else {
            self.min_lat.append_null();
        }

        if let Some(lon) = cs.min_lon {
            self.min_lon.append_value(lon);
        } else {
            self.min_lon.append_null();
        }

        if let Some(lat) = cs.max_lat {
            self.max_lat.append_value(lat);
        } else {
            self.max_lat.append_null();
        }

        if let Some(lon) = cs.max_lon {
            self.max_lon.append_value(lon);
        } else {
            self.max_lon.append_null();
        }

        self.num_changes.append_value(cs.num_changes);
        self.comments_count.append_value(cs.comments_count);

        if let Some(ref description) = cs.description {
            self.description.append_value(description);
        } else {
            self.description.append_null();
        }

        self.len += 1;
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn finish_batch(&mut self, schema: &Arc<Schema>) -> Result<RecordBatch> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.id.finish()),
            Arc::new(self.created_at.finish()),
            Arc::new(self.closed_at.finish()),
            Arc::new(self.open.finish()),
            Arc::new(self.user.finish()),
            Arc::new(self.uid.finish()),
            Arc::new(self.min_lat.finish()),
            Arc::new(self.min_lon.finish()),
            Arc::new(self.max_lat.finish()),
            Arc::new(self.max_lon.finish()),
            Arc::new(self.num_changes.finish()),
            Arc::new(self.comments_count.finish()),
            Arc::new(self.description.finish()),
        ];

        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        self.len = 0;
        Ok(batch)
    }
}

fn parse_timestamp(s: &str) -> Result<i64> {
    let dt = chrono::DateTime::parse_from_rfc3339(s)
        .with_context(|| format!("Failed to parse timestamp: {}", s))?;
    Ok(dt.timestamp_millis())
}

fn parse_changeset_element(e: &BytesStart) -> Result<Changeset> {
    let mut changeset = Changeset::default();

    for attr in e.attributes() {
        let attr = attr?;
        let key = attr.key.as_ref();
        let value = attr.unescape_value()?;

        match key {
            b"id" => changeset.id = value.as_ref().parse()?,
            b"created_at" => changeset.created_at = Some(parse_timestamp(value.as_ref())?),
            b"closed_at" => changeset.closed_at = Some(parse_timestamp(value.as_ref())?),
            b"open" => changeset.open = value == "true",
            b"user" => changeset.user = Some(value.to_string()),
            b"uid" => changeset.uid = Some(value.as_ref().parse()?),
            b"min_lat" => changeset.min_lat = Some(value.as_ref().parse()?),
            b"min_lon" => changeset.min_lon = Some(value.as_ref().parse()?),
            b"max_lat" => changeset.max_lat = Some(value.as_ref().parse()?),
            b"max_lon" => changeset.max_lon = Some(value.as_ref().parse()?),
            b"num_changes" => changeset.num_changes = value.as_ref().parse()?,
            b"comments_count" => changeset.comments_count = value.as_ref().parse()?,
            _ => {}
        }
    }

    Ok(changeset)
}

fn apply_changeset_tag(e: &BytesStart, changeset: &mut Changeset) -> Result<()> {
    let mut key = None;
    let mut value = None;

    for attr in e.attributes() {
        let attr = attr?;
        match attr.key.as_ref() {
            b"k" => key = Some(attr.unescape_value()?.into_owned()),
            b"v" => value = Some(attr.unescape_value()?.into_owned()),
            _ => {}
        }
    }

    if let (Some(k), Some(v)) = (key, value) {
        if k == "comment" {
            changeset.description = Some(v);
        }
    }

    Ok(())
}

fn parse_changeset_body<R: std::io::BufRead>(
    reader: &mut Reader<R>,
    changeset: &mut Changeset,
    buf: &mut Vec<u8>,
) -> Result<()> {
    loop {
        match reader.read_event_into(buf) {
            Ok(Event::Empty(ref e)) => {
                if e.name().as_ref() == b"tag" {
                    apply_changeset_tag(e, changeset)?;
                }
            }
            Ok(Event::Start(e)) => {
                let owned = e.into_owned();
                let tag_name = owned.name().as_ref().to_vec();
                if owned.name().as_ref() == b"tag" {
                    apply_changeset_tag(&owned, changeset)?;
                }
                // Drop owned before handing mutable access back to reader/BufRead
                drop(owned);
                reader.read_to_end_into(QName(&tag_name), buf)?;
            }
            Ok(Event::End(ref e)) if e.name().as_ref() == b"changeset" => break,
            Ok(Event::Eof) => {
                return Err(anyhow::anyhow!(
                    "Unexpected EOF while parsing changeset body"
                ))
            }
            Ok(_) => {}
            Err(err) => return Err(err.into()),
        }
        buf.clear();
    }

    Ok(())
}

fn parse_and_write_changesets<R: std::io::Read>(
    reader: R,
    writer: &mut ArrowWriter<File>,
    schema: &Arc<Schema>,
    batch_size: usize,
    continue_on_error: bool,
) -> Result<usize> {
    // Use a large buffer (1MB) to avoid splitting XML tags across read boundaries
    let buffered_reader = BufReader::with_capacity(1024 * 1024, reader);
    let mut xml_reader = Reader::from_reader(buffered_reader);
    xml_reader.config_mut().trim_text_start = true;
    xml_reader.config_mut().trim_text_end = true;
    xml_reader.config_mut().check_end_names = false; // More lenient parsing
    xml_reader.config_mut().check_comments = false; // Don't validate comments

    let mut buf = Vec::new();
    let mut count = 0;
    let mut batch_num = 0;
    let mut last_changeset_id = 0i64;

    let mut temp_buf = Vec::new();
    let effective_batch_size = batch_size.max(1);
    let mut builders = BatchBuilders::with_capacity(effective_batch_size);

    let mut process_changeset = |changeset: Changeset| -> Result<()> {
        last_changeset_id = changeset.id;
        builders.append(&changeset);
        count += 1;

        if builders.len() >= effective_batch_size {
            batch_num += 1;
            let batch = builders.finish_batch(schema)?;
            let rows = batch.num_rows();
            println!(
                "Writing batch {} with {} rows (total: {})...",
                batch_num, rows, count
            );
            writer.write(&batch)?;
        }

        Ok(())
    };

    loop {
        let position = xml_reader.buffer_position();
        match xml_reader.read_event_into(&mut buf) {
            Ok(Event::Empty(ref e)) if e.name().as_ref() == b"changeset" => {
                let changeset = parse_changeset_element(e)?;
                process_changeset(changeset)?;
            }
            Ok(Event::Start(ref e)) if e.name().as_ref() == b"changeset" => {
                let mut changeset = parse_changeset_element(e)?;
                temp_buf.clear();
                parse_changeset_body(&mut xml_reader, &mut changeset, &mut temp_buf)?;
                temp_buf.clear();
                process_changeset(changeset)?;
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                eprintln!("\n=== XML PARSE ERROR ===");
                eprintln!("Position: {}", position);
                eprintln!("Changesets processed: {}", count);
                eprintln!("Last changeset ID: {}", last_changeset_id);
                eprintln!("Error: {}", e);
                eprintln!("\nBuffer content at error (first 500 bytes):");
                eprintln!("{}", String::from_utf8_lossy(&buf[..buf.len().min(500)]));
                eprintln!("\nBuffer content at error (last 500 bytes):");
                let start = if buf.len() > 500 { buf.len() - 500 } else { 0 };
                eprintln!("{}", String::from_utf8_lossy(&buf[start..]));
                eprintln!("======================\n");

                if continue_on_error {
                    eprintln!("Continuing with {} successfully parsed changesets...", count);
                    break;
                } else {
                    return Err(anyhow::anyhow!("Error parsing XML: {}. Use --continue-on-error to save partial results.", e));
                }
            }
            _ => {}
        }
        buf.clear();
    }

    // Process remaining changesets
    if !builders.is_empty() {
        batch_num += 1;
        let batch = builders.finish_batch(schema)?;
        let rows = batch.num_rows();
        println!(
            "Writing final batch {} with {} rows (total: {})...",
            batch_num, rows, count
        );
        writer.write(&batch)?;
    }

    Ok(count)
}

fn create_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "closed_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("open", DataType::Boolean, false),
        Field::new("user", DataType::Utf8, true),
        Field::new("uid", DataType::Int64, true),
        Field::new("min_lat", DataType::Float64, true),
        Field::new("min_lon", DataType::Float64, true),
        Field::new("max_lat", DataType::Float64, true),
        Field::new("max_lon", DataType::Float64, true),
        Field::new("num_changes", DataType::UInt32, false),
        Field::new("comments_count", DataType::UInt32, false),
        Field::new("description", DataType::Utf8, true),
    ]))
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("Reading from: {}", args.input);
    println!("Writing to: {}", args.output);

    // Set up parquet writer first
    let output_file = File::create(&args.output)
        .with_context(|| format!("Failed to create output file: {}", args.output))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let schema = create_schema();
    let mut writer = ArrowWriter::try_new(output_file, schema.clone(), Some(props))?;

    // Stream parse and write
    let file = File::open(&args.input)
        .with_context(|| format!("Failed to open input file: {}", args.input))?;

    let total_count = if args.input.ends_with(".bz2") {
        println!("Detected bzip2 multi-stream compressed file");
        let decoder = MultiBzDecoder::new(file);
        parse_and_write_changesets(
            decoder,
            &mut writer,
            &schema,
            args.batch_size,
            args.continue_on_error,
        )?
    } else {
        parse_and_write_changesets(
            file,
            &mut writer,
            &schema,
            args.batch_size,
            args.continue_on_error,
        )?
    };

    writer.close()?;

    println!("Successfully wrote {} changesets to {}", total_count, args.output);

    Ok(())
}
