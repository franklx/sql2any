use anyhow::Result;
use clap::Parser;
use enum_map::{enum_map, Enum, EnumMap};
use num_traits::ToPrimitive;
use rust_xlsxwriter::Format;
use rust_xlsxwriter::{ColNum, RowNum, Workbook, Worksheet};
use sqlx::database::HasArguments;
use sqlx::types::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::types::{Decimal, JsonValue};
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

async fn run<'a, DB, E>(bk: &'a str, db: &'a mut E, sql: &'a str, output: impl AsRef<Path>) -> Result<()>
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
        let columns: &[DB::Column] = result[0].columns();
        let convs = columns
            .iter()
            .enumerate()
            .inspect(|(c, col)| {
                ws.write_with_format(r, *c as ColNum, col.name(), &xf[XF::Bold]).unwrap();
            })
            .map::<ConvFn<DB::Row>, _>(|(_c, col)| match col.type_info().name().to_lowercase().as_str() {
                "string" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "char" | "bpchar" => {
                    xlsx_write!(&str)
                }
                "tinyint" => xlsx_write!(i8, XF::Int),
                "int2" | "smallint" => xlsx_write!(i16, XF::Int),
                "int4" | "int" | "mediumint" => xlsx_write!(i32, XF::Int),
                "int8" | "bigint" => |r, c, ws, rw, fm| {
                    ws.write_with_format(r, c, rw.get::<i64, _>(c as usize) as f64, &fm[XF::Int])?;
                    Ok(())
                },
                //"tinyint unsigned" => xlsx_write!(u8, XF::Int),
                //"smallint unsigned" => xlsx_write!(u16, XF::Int),
                //"int unsigned" | "mediumint unsigned" => xlsx_write!(u32, XF::Int),
                //"bigint unsigned" => |r, c, ws, rw, fm| {
                //    ws.write_with_format(r, c, rw.get::<u64, _>(c as usize) as f64, &fm[XF::Int])?;
                //    Ok(())
                //},
                "float4" | "float" => xlsx_write!(f32, XF::Eur),
                "float8" | "double" => xlsx_write!(f64, XF::Eur),
                "decimal" | "numeric" => |r, c, ws, rw, fm| {
                    ws.write_with_format(r, c, rw.get::<Decimal, _>(c as usize).to_f64().unwrap(), &fm[XF::Eur])?;
                    Ok(())
                },
                // binary(16) => uuid
                "json" | "jsonb" => |r, c, ws, rw, _fm| {
                    ws.write(r, c, rw.get::<JsonValue, _>(c as usize).to_string())?;
                    Ok(())
                },
                "bool" | "boolean" => xlsx_write!(bool),
                "date" => xlsx_write!(Option<NaiveDate>, XF::Date),
                "time" => xlsx_write!(Option<NaiveTime>, XF::Time),
                "datetime" => xlsx_write!(Option<NaiveDateTime>, XF::Stamp),
                "timestamp" if bk == "postgres" => xlsx_write!(Option<NaiveDateTime>, XF::Stamp),
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
                "timestamptz" | "timestamp" if bk == "mysql" => |r, c, ws, rw, fm| {
                    if let Some(v) = rw.get::<Option<DateTime<Local>>, _>(c as usize) {
                        ws.write_with_format(r, c, &v.naive_local(), &fm[XF::Stamp])?;
                    }
                    Ok(())
                },
                typ => {
                    eprintln!("Unsupported type {typ:?}");
                    xlsx_write!()
                }
            })
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let db_url = Url::parse(
        args.url.unwrap_or_else(|| var("DATABASE_URL").expect("DATABASE_URL must be set if url not provided")).as_ref(),
    )
    .expect("Invalid url");
    match db_url.scheme() {
        bk @ "postgres" => {
            let mut db = sqlx::PgConnection::connect(db_url.as_str()).await?;
            run(bk, &mut db, &args.query, &args.output).await?;
        }
        bk @ "mysql" => {
            let mut db = sqlx::MySqlConnection::connect(db_url.as_str()).await?;
            run(bk, &mut db, &args.query, &args.output).await?;
        }
        scheme => panic!("Unknown driver {scheme}"),
    }
    Ok(())
}
