use anyhow::Result;
use clap::Parser;
use enum_map::{enum_map, Enum, EnumMap};
use rust_xlsxwriter::Format;
use rust_xlsxwriter::{ColNum, RowNum, Workbook, Worksheet};
use sqlx::database::HasArguments;
use sqlx::types::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::Connection;
use sqlx::Row;
use sqlx::{Column, ColumnIndex, Database, Decode, Executor, IntoArguments, Type, TypeInfo};
use std::env::var;
use std::path::Path;
use url::Url;

type FmtMap = EnumMap<XF, Format>;
type ConvFn<'a, R> = fn(RowNum, ColNum, &mut Worksheet, &'a R, &FmtMap) -> Result<()>;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    url: Option<String>,

    #[arg(short, long)]
    output: String,

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

async fn run<'a, DB, E>(db: &'a mut E, sql: &'a str, output: impl AsRef<Path>) -> Result<()>
where
    DB: Database,
    <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    &'a mut E: Executor<'a, Database = DB>,
    for<'b> i8: Decode<'b, DB> + Type<DB>,
    for<'b> i16: Decode<'b, DB> + Type<DB>,
    for<'b> i32: Decode<'b, DB> + Type<DB>,
    for<'b> i64: Decode<'b, DB> + Type<DB>,
    for<'b> f32: Decode<'b, DB> + Type<DB>,
    for<'b> f64: Decode<'b, DB> + Type<DB>,
    for<'b> &'b str: Decode<'b, DB> + Type<DB>,
    for<'b> bool: Decode<'b, DB> + Type<DB>,
    for<'b> NaiveDate: Decode<'b, DB> + Type<DB>,
    for<'b> NaiveDateTime: Decode<'b, DB> + Type<DB>,
    for<'b> NaiveTime: Decode<'b, DB> + Type<DB>,
    for<'b> DateTime<Local>: Decode<'b, DB> + Type<DB>,
    usize: ColumnIndex<DB::Row>,
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
    let result = sqlx::query(sql).fetch_all(db).await.unwrap();
    if !result.is_empty() {
        let r = 0;
        let columns = result[0].columns();
        let convs = columns
            .iter()
            .enumerate()
            .inspect(|(c, col)| {
                ws.write_with_format(r, *c as ColNum, col.name(), &xf[XF::Bold]).unwrap();
            })
            .map::<ConvFn<DB::Row>, _>(|(_c, col)| match col.type_info().name().to_lowercase().as_str() {
                "string" | "varchar" | "text" | "char" => xlsx_write!(&str),
                "tinyint" => xlsx_write!(i8, XF::Int),
                "int2" | "smallint" => xlsx_write!(i16, XF::Int),
                "int4" | "bigint" => xlsx_write!(i32, XF::Int),
                "int8" => xlsx_write!(f64, XF::Int),
                "float4" | "float" => xlsx_write!(f32, XF::Eur),
                "float8" | "double" => xlsx_write!(f64, XF::Eur),
                "bool" => xlsx_write!(bool),
                "date" => xlsx_write!(Option<NaiveDate>, XF::Date),
                "time" => xlsx_write!(Option<NaiveTime>, XF::Time),
                "datetime" => xlsx_write!(Option<NaiveDateTime>, XF::Stamp),
                "timestamp" => |r, c, ws, rw, fm| {
                    if let Some(v) = rw.get::<Option<DateTime<Local>>, _>(c as usize) {
                        ws.write_with_format(r, c, &v.naive_local(), &fm[XF::Stamp])?;
                    }
                    Ok(())
                },
                typ => {
                    eprintln!("Unknown type {typ:?}");
                    xlsx_write!()
                }
            })
            .collect::<Vec<_>>();
        for (r, rw) in result.iter().enumerate() {
            for (c, conv) in convs.iter().enumerate() {
                conv((r + 1) as RowNum, c as ColNum, ws, rw, &xf)?;
            }
        }
        wb.save(output)?;
    }
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
            run(&mut db, &args.query, &args.output).await?;
        }
        "mysql" => {
            let mut db = sqlx::MySqlConnection::connect(db_url.as_str()).await?;
            run(&mut db, &args.query, &args.output).await?;
        }
        scheme => panic!("Unknown driver {scheme}"),
    }
    Ok(())
}
