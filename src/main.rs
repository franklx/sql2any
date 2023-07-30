use anyhow::Result;
use clap::Parser;
use enum_map::{enum_map, Enum, EnumMap};
use num_traits::ToPrimitive;
use rust_xlsxwriter::Format;
use rust_xlsxwriter::{ColNum, RowNum, Workbook, Worksheet};
use serde_json::{Map, Value};
use sqlx::database::HasArguments;
use sqlx::mysql::MySqlColumn;
use sqlx::postgres::PgColumn;
use sqlx::types::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::types::{Decimal, JsonValue};
use sqlx::Row;
use sqlx::Connection;
use sqlx::{Column, ColumnIndex, Database, Decode, Executor, IntoArguments, Type, TypeInfo};
use std::env::var;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use url::Url;

type XlsxFmtMap = EnumMap<XF, Format>;
type XlsxConvFn<'a, R> = fn(RowNum, ColNum, &mut Worksheet, &'a R, &XlsxFmtMap) -> Result<()>;

#[derive(Parser, Debug)]
struct Args {
    /// Database url to connect to
    #[arg(short, long)]
    url: Option<String>,

    /// Output filename in xlsx format
    #[arg(short, long)]
    output: String,

    /// SQL query to execute
    #[arg()]
    query: String,
}

#[derive(Enum)]
enum XF {
    Bold,
    Int,
    Eur,
    Date,
    Time,
    Stamp,
}

#[macro_export]
macro_rules! xlsx_write {
    () => {
        |_r, _c, _ws, _rw, _fm| Ok(())
    };
    ($ty:ty) => {
        |r, c, ws, rw, _fm| {
            ws.write(r, c, rw.get::<$ty, _>(c as usize))?;
            Ok(())
        }
    };
    (Option<$ty:ty>, $fmt:path) => {
        |r, c, ws, rw, fm| {
            if let Some(v) = rw.get::<Option<$ty>, _>(c as usize) {
                ws.write_with_format(r, c, &v, &fm[$fmt])?;
            }
            Ok(())
        }
    };
    ($ty:ty, $fmt:path) => {
        |r, c, ws, rw, fm| {
            ws.write_with_format(r, c, rw.get::<$ty, _>(c as usize), &fm[$fmt])?;
            Ok(())
        }
    };
}

#[macro_export]
macro_rules! json_write {
    ($ty:ty) => {
        |c, rw| rw.get::<$ty, _>(c).into()
    };
}

#[macro_export]
macro_rules! json_write_date {
    ($ty:ty) => {
        //|c, rw| rw.get::<$ty, _>(c).map(|d| d.format("%+").to_string()).into()
        |c, rw| rw.get::<$ty, _>(c).map(|d| d.to_string()).into()
    };
}

pub enum FieldKind {
    INT8,
    INT16,
    INT32,
    INT64,
    UINT8,
    UINT16,
    UINT32,
    UINT64,
    FLOAT32,
    FLOAT64,
    STR,
    BOOL,
    DECIMAL,
    DATE,       //Option<NaiveDate>
    TIME,       //Option<NaiveTime>
    DATETIME,   //Option<NaiveDateTime>
    DATETIMETZ, //Option<DateTime<Local>>
    JSON,       //JsonValue
    UNKNOWN(String),
}

fn get_common_type(name: &str) -> FieldKind {
    match name {
        "string" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "char" | "bpchar" => FieldKind::STR,
        "tinyint" => FieldKind::INT8,
        "int2" | "smallint" => FieldKind::INT16,
        "int4" | "int" | "mediumint" => FieldKind::INT32,
        "int8" | "bigint" => FieldKind::INT64,
        "tinyint unsigned" => FieldKind::UINT8,
        "smallint unsigned" => FieldKind::UINT16,
        "int unsigned" | "mediumint unsigned" => FieldKind::UINT32,
        "bigint unsigned" => FieldKind::UINT64,
        "float4" | "float" => FieldKind::FLOAT32,
        "float8" | "double" => FieldKind::FLOAT64,
        "decimal" | "numeric" => FieldKind::DECIMAL,
        // binary(16) => uuid
        "json" | "jsonb" => FieldKind::JSON,
        "bool" | "boolean" => FieldKind::BOOL,
        "date" => FieldKind::DATE,
        "time" => FieldKind::TIME,
        "datetime" => FieldKind::DATETIME,
        "timestamptz" => FieldKind::DATETIMETZ,
        //"timetz" // DEPRECATED
        //"money" // DEPRECATED
        //"bit"
        //"varbit"
        //"varbinary"
        //"tinyblob"
        //"blob"
        //"mediumblob"
        //"longblob"
        //"year"
        //"set"
        //"enum"
        typ => FieldKind::UNKNOWN(typ.to_string()),
    }
}

pub struct Field {
    name: String,
    kind: FieldKind,
}

impl From<&PgColumn> for Field {
    fn from(col: &PgColumn) -> Self {
        let kind = match col.type_info().name().to_lowercase().as_str() {
            "timestamp" => FieldKind::DATETIME,
            name => get_common_type(name),
        };
        Self { name: col.name().to_string(), kind }
    }
}

impl From<&MySqlColumn> for Field {
    fn from(col: &MySqlColumn) -> Self {
        let kind = match col.type_info().name().to_lowercase().as_str() {
            "timestamp" => FieldKind::DATETIMETZ,
            name => get_common_type(name),
        };
        Self { name: col.name().to_string(), kind }
    }
}

type JsonMap = Map<String, Value>;
type JsonConvFn<'a, R> = fn(usize, &'a R) -> Value;

impl Field {
    fn to_json<DB>(&self) -> JsonConvFn<DB::Row>
    where
        DB: Database,
        for<'b> i8: Decode<'b, DB> + Type<DB>,
        for<'b> i16: Decode<'b, DB> + Type<DB>,
        for<'b> i32: Decode<'b, DB> + Type<DB>,
        for<'b> i64: Decode<'b, DB> + Type<DB>,
        //for<'b> u8: Decode<'b, DB> + Type<DB>,
        //for<'b> u16: Decode<'b, DB> + Type<DB>,
        //for<'b> u32: Decode<'b, DB> + Type<DB>,
        //for<'b> u64: Decode<'b, DB> + Type<DB>,
        for<'b> f32: Decode<'b, DB> + Type<DB>,
        for<'b> f64: Decode<'b, DB> + Type<DB>,
        for<'b> &'b str: Decode<'b, DB> + Type<DB>,
        for<'b> bool: Decode<'b, DB> + Type<DB>,
        for<'b> NaiveDate: Decode<'b, DB> + Type<DB>,
        for<'b> NaiveDateTime: Decode<'b, DB> + Type<DB>,
        for<'b> NaiveTime: Decode<'b, DB> + Type<DB>,
        for<'b> DateTime<Local>: Decode<'b, DB> + Type<DB>,
        for<'b> Decimal: Decode<'b, DB> + Type<DB>,
        for<'b> JsonValue: Decode<'b, DB> + Type<DB>,
        usize: ColumnIndex<DB::Row>,
    {
        match self.kind {
            FieldKind::INT8 => json_write!(i8),
            FieldKind::INT16 => json_write!(i16),
            FieldKind::INT32 => json_write!(i32),
            FieldKind::INT64 => json_write!(i64),
            FieldKind::UINT8 => todo!(),
            FieldKind::UINT16 => todo!(),
            FieldKind::UINT32 => todo!(),
            FieldKind::UINT64 => todo!(),
            FieldKind::FLOAT32 => json_write!(f32),
            FieldKind::FLOAT64 => json_write!(f64),
            FieldKind::STR => json_write!(&str),
            FieldKind::BOOL => json_write!(bool),
            FieldKind::DECIMAL => |c, rw|
                rw.get::<Decimal, _>(c).to_f64().unwrap().into(),
            FieldKind::DATE => json_write_date!(Option<NaiveDate>),
            FieldKind::TIME => json_write_date!(Option<NaiveTime>),
            FieldKind::DATETIME => json_write_date!(Option<NaiveDateTime>),
            FieldKind::DATETIMETZ => json_write_date!(Option<DateTime<Local>>),
            FieldKind::JSON => json_write!(JsonValue),
            FieldKind::UNKNOWN(_) => todo!(),
        }
    }

    fn to_xlsx<DB>(&self) -> XlsxConvFn<DB::Row>
    where
        DB: Database,
        for<'b> i8: Decode<'b, DB> + Type<DB>,
        for<'b> i16: Decode<'b, DB> + Type<DB>,
        for<'b> i32: Decode<'b, DB> + Type<DB>,
        for<'b> i64: Decode<'b, DB> + Type<DB>,
        //for<'b> u8: Decode<'b, DB> + Type<DB>,
        //for<'b> u16: Decode<'b, DB> + Type<DB>,
        //for<'b> u32: Decode<'b, DB> + Type<DB>,
        //for<'b> u64: Decode<'b, DB> + Type<DB>,
        for<'b> f32: Decode<'b, DB> + Type<DB>,
        for<'b> f64: Decode<'b, DB> + Type<DB>,
        for<'b> &'b str: Decode<'b, DB> + Type<DB>,
        for<'b> bool: Decode<'b, DB> + Type<DB>,
        for<'b> NaiveDate: Decode<'b, DB> + Type<DB>,
        for<'b> NaiveDateTime: Decode<'b, DB> + Type<DB>,
        for<'b> NaiveTime: Decode<'b, DB> + Type<DB>,
        for<'b> DateTime<Local>: Decode<'b, DB> + Type<DB>,
        for<'b> Decimal: Decode<'b, DB> + Type<DB>,
        for<'b> JsonValue: Decode<'b, DB> + Type<DB>,
        usize: ColumnIndex<DB::Row>,
    {
        match self.kind {
            FieldKind::INT8 => xlsx_write!(i8, XF::Int),
            FieldKind::INT16 => xlsx_write!(i16, XF::Int),
            FieldKind::INT32 => xlsx_write!(i32, XF::Int),
            FieldKind::INT64 => |r, c, ws, rw, fm| {
                ws.write_with_format(r, c, rw.get::<i64, _>(c as usize) as f64, &fm[XF::Int])?;
                Ok(())
            },
            FieldKind::UINT8 => todo!(),
            FieldKind::UINT16 => todo!(),
            FieldKind::UINT32 => todo!(),
            FieldKind::UINT64 => todo!(),
            FieldKind::FLOAT32 => xlsx_write!(f32, XF::Eur),
            FieldKind::FLOAT64 => xlsx_write!(f64, XF::Eur),
            FieldKind::STR => xlsx_write!(&str),
            FieldKind::BOOL => xlsx_write!(bool),
            FieldKind::DECIMAL => |r, c, ws, rw, fm| {
                ws.write_with_format(r, c, rw.get::<Decimal, _>(c as usize).to_f64().unwrap(), &fm[XF::Eur])?;
                Ok(())
            },
            FieldKind::DATE => xlsx_write!(Option<NaiveDate>, XF::Date),
            FieldKind::TIME => xlsx_write!(Option<NaiveTime>, XF::Time),
            FieldKind::DATETIME => xlsx_write!(Option<NaiveDateTime>, XF::Stamp),
            FieldKind::DATETIMETZ => |r, c, ws, rw, fm| {
                if let Some(v) = rw.get::<Option<DateTime<Local>>, _>(c as usize) {
                    ws.write_with_format(r, c, &v.naive_local(), &fm[XF::Stamp])?;
                }
                Ok(())
            },
            FieldKind::JSON => |r, c, ws, rw, _fm| {
                ws.write(r, c, rw.get::<JsonValue, _>(c as usize).to_string())?;
                Ok(())
            },
            FieldKind::UNKNOWN(_) => todo!(),
        }
    }
}

async fn to_xlsx<'a, DB, E>(db: &'a mut E, sql: &'a str, output: impl AsRef<Path>) -> Result<()>
where
    DB: Database,
    <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    &'a mut E: Executor<'a, Database = DB>,
    for<'b> i8: Decode<'b, DB> + Type<DB>,
    for<'b> i16: Decode<'b, DB> + Type<DB>,
    for<'b> i32: Decode<'b, DB> + Type<DB>,
    for<'b> i64: Decode<'b, DB> + Type<DB>,
    //for<'b> u8: Decode<'b, DB> + Type<DB>,
    //for<'b> u16: Decode<'b, DB> + Type<DB>,
    //for<'b> u32: Decode<'b, DB> + Type<DB>,
    //for<'b> u64: Decode<'b, DB> + Type<DB>,
    for<'b> f32: Decode<'b, DB> + Type<DB>,
    for<'b> f64: Decode<'b, DB> + Type<DB>,
    for<'b> &'b str: Decode<'b, DB> + Type<DB>,
    for<'b> bool: Decode<'b, DB> + Type<DB>,
    for<'b> NaiveDate: Decode<'b, DB> + Type<DB>,
    for<'b> NaiveDateTime: Decode<'b, DB> + Type<DB>,
    for<'b> NaiveTime: Decode<'b, DB> + Type<DB>,
    for<'b> DateTime<Local>: Decode<'b, DB> + Type<DB>,
    for<'b> Decimal: Decode<'b, DB> + Type<DB>,
    for<'b> JsonValue: Decode<'b, DB> + Type<DB>,
    usize: ColumnIndex<DB::Row>,
    for<'b> &'b DB::Column: Into<Field>,
{
    let xf = enum_map! {
        XF::Bold => Format::new().set_bold(),
        XF::Int => Format::new().set_num_format("#,##0"),
        XF::Eur => Format::new().set_num_format("#,##0.00"),
        XF::Date => Format::new().set_num_format("dd/mm/yyyy"),
        XF::Time => Format::new().set_num_format("hh:mm"),
        XF::Stamp => Format::new().set_num_format("dd/mm/yyyy hh:mm:ss"),
    };
    let mut wb = Workbook::new();
    let ws = wb.add_worksheet();
    ws.set_freeze_panes(1, 0)?;
    let result = sqlx::query(sql).fetch_all(db).await.unwrap();
    if !result.is_empty() {
        let r = 0;
        let columns: Vec<Field> = result[0].columns().iter().map(|c| c.into()).collect();
        let convs = columns
            .iter()
            .enumerate()
            .inspect(|(c, col)| {
                ws.write_with_format(r, *c as ColNum, &col.name, &xf[XF::Bold]).unwrap();
            })
            .map(|(_c, col)| col.to_xlsx())
            .collect::<Vec<_>>();
        for (r, rw) in result.iter().enumerate() {
            for (c, conv) in convs.iter().enumerate() {
                conv((r + 1) as RowNum, c as ColNum, ws, rw, &xf)?;
            }
        }
        ws.autofilter(0, 0, (result.len() as u32) - 1, (columns.len() as u16) - 1)?;
        ws.autofit();
        wb.save(output)?;
    }
    Ok(())
}

async fn to_json<'a, DB, E>(db: &'a mut E, sql: &'a str, output: impl AsRef<Path>) -> Result<()>
where
    DB: Database,
    <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    &'a mut E: Executor<'a, Database = DB>,
    for<'b> i8: Decode<'b, DB> + Type<DB>,
    for<'b> i16: Decode<'b, DB> + Type<DB>,
    for<'b> i32: Decode<'b, DB> + Type<DB>,
    for<'b> i64: Decode<'b, DB> + Type<DB>,
    //for<'b> u8: Decode<'b, DB> + Type<DB>,
    //for<'b> u16: Decode<'b, DB> + Type<DB>,
    //for<'b> u32: Decode<'b, DB> + Type<DB>,
    //for<'b> u64: Decode<'b, DB> + Type<DB>,
    for<'b> f32: Decode<'b, DB> + Type<DB>,
    for<'b> f64: Decode<'b, DB> + Type<DB>,
    for<'b> &'b str: Decode<'b, DB> + Type<DB>,
    for<'b> bool: Decode<'b, DB> + Type<DB>,
    for<'b> NaiveDate: Decode<'b, DB> + Type<DB>,
    for<'b> NaiveDateTime: Decode<'b, DB> + Type<DB>,
    for<'b> NaiveTime: Decode<'b, DB> + Type<DB>,
    for<'b> DateTime<Local>: Decode<'b, DB> + Type<DB>,
    for<'b> Decimal: Decode<'b, DB> + Type<DB>,
    for<'b> JsonValue: Decode<'b, DB> + Type<DB>,
    usize: ColumnIndex<DB::Row>,
    for<'b> &'b DB::Column: Into<Field>,
{
    let mut jf = File::create(output)?;
    writeln!(jf, "[")?;
    let result = sqlx::query(sql).fetch_all(db).await.unwrap();
    if !result.is_empty() {
        let columns: Vec<Field> = result[0].columns().iter().map(|c| c.into()).collect();
        let convs = columns
            .iter()
            .enumerate()
            .map(|(_c, col)| col.to_json())
            .collect::<Vec<_>>();
        for rw in result.iter() {
            let ji = convs.iter().enumerate().map(|(c, conv)| (columns[c].name.clone(), conv(c, rw)));
            let jr = JsonMap::from_iter(ji);
            serde_json::to_writer(&jf, &jr)?;
            writeln!(jf, ",")?;
        }
    }
    writeln!(jf, "]")?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let db_url = Url::parse(
        args.url.unwrap_or_else(|| var("DATABASE_URL").expect("DATABASE_URL must be set if url not provided")).as_ref(),
    )
    .expect("Invalid url");
    match db_url.scheme() {
        "postgres" => {
            let mut db = sqlx::PgConnection::connect(db_url.as_str()).await?;
            to_xlsx(&mut db, &args.query, &args.output).await?;
            to_json(&mut db, &args.query, &args.output).await?;
        }
        "mysql" => {
            let mut db = sqlx::MySqlConnection::connect(db_url.as_str()).await?;
            to_xlsx(&mut db, &args.query, &args.output).await?;
            to_json(&mut db, &args.query, &args.output).await?;
        }
        scheme => panic!("Unknown driver {scheme}"),
    }
    Ok(())
}
