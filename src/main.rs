use anyhow::Result;
use enum_map::{enum_map, Enum, EnumMap};
use rust_xlsxwriter::{ColNum, RowNum, Workbook, Worksheet};
use rust_xlsxwriter::Format;
use sqlx::types::chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::Column;
use sqlx::Connection;
use sqlx::PgConnection;
use sqlx::TypeInfo;
use sqlx::{postgres::PgRow, Row};
use std::env::var;
use std::path::Path;
use clap::Parser;

type FmtMap = EnumMap<XF, Format>;
type ConvFn = fn(RowNum, ColNum, &mut Worksheet, &PgRow, &FmtMap) -> Result<()>;

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

async fn run(db: &mut PgConnection, sql: &str, output: impl AsRef<Path>) -> Result<()> {
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
            .map::<ConvFn, _>(|(_c, col)| match col.type_info().name().to_lowercase().as_str() {
                "string" | "varchar" | "text" | "char" => xlsx_write!(&str),
                "int2" => xlsx_write!(i16, XF::Int),
                "int4" => xlsx_write!(i32, XF::Int),
                //"int8" => xlsx_write!(i64, Int),
                "float4" => xlsx_write!(f32, XF::Eur),
                "float8" => xlsx_write!(f64, XF::Eur),
                "bool" => xlsx_write!(bool),
                "date" => xlsx_write!(Option<NaiveDate>, XF::Date),
                "time" => xlsx_write!(Option<NaiveTime>, XF::Time),
                "timestamp" => xlsx_write!(Option<NaiveDateTime>, XF::Stamp),
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
    }
    wb.save(output)?;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let db_url = args.url.unwrap_or_else(|| var("DATABASE_URL").expect("DATABASE_URL must be set if url not provided"));
    let mut db = sqlx::PgConnection::connect(&db_url).await?;
    run(&mut db, &args.query, &args.output).await?;
    Ok(())
}
