use anyhow::Result;
use rust_xlsxwriter::{Format, XlsxError};
use rust_xlsxwriter::{ColNum, RowNum, Workbook, Worksheet};
use sqlx::AnyConnection;
use sqlx::AnyPool;
use sqlx::Column;
use sqlx::Connection;
use sqlx::Database;
use sqlx::PgConnection;
use sqlx::TypeInfo;
use sqlx::types::chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::{postgres::PgRow, Row};
use std::env::var;
use std::ops::Deref;
use std::path::PathBuf;
use std::str::FromStr;
use enum_map::{Enum, enum_map, EnumMap};

type FmtMap = EnumMap<XF, Format>;
type ConvFn = fn(RowNum, ColNum, &mut Worksheet, &PgRow, &FmtMap) -> Result<()>;

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
        |_r, _c, _ws, _rw, _fm| {
            Ok(())
        }
    };
    ($ty:ty) => {
        |r, c, ws, rw, _fm| {
            ws.write(r, c, rw.get::<$ty, _>(c as usize))?; Ok(())
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
            ws.write_with_format(r, c, rw.get::<$ty, _>(c as usize), &fm[$fmt])?; Ok(())
        }
    };
}


async fn test_clean(db: &mut PgConnection) -> Result<()> {
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
    let result = sqlx::query("SELECT * FROM sintorip.artico").fetch_all(db).await.unwrap();
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
                },
                /*
                match col.type_info().kind() {
                    sqlx::postgres::any::AnyTypeInfoKind::Null => todo!(),
                    sqlx::postgres::any::AnyTypeInfoKind::Bool => todo!(),
                    sqlx::postgres::any::AnyTypeInfoKind::SmallInt => todo!(),
                    sqlx::postgres::any::AnyTypeInfoKind::Integer => todo!(),
                    sqlx::postgres::any::AnyTypeInfoKind::BigInt => todo!(),
                    sqlx::postgres::any::AnyTypeInfoKind::Real => todo!(),
                    sqlx::postgres::any::AnyTypeInfoKind::Double => todo!(),
                    sqlx::postgres::any::AnyTypeInfoKind::Text => todo!(),
                    sqlx::postgres::any::AnyTypeInfoKind::Blob => todo!(),
                };
                */
            })
            .collect::<Vec<_>>();
        for (r, rw) in result.iter().enumerate() {
            for (c, conv) in convs.iter().enumerate() {
                conv((r + 1) as RowNum, c as ColNum, ws, rw, &xf)?;
            }
        }
    }
    wb.save("test.xlsx")?;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let db_url = var("DATABASE_URL").expect("DATABASE_URL must be set");
    sqlx::any::install_default_drivers();
    //let mut db = sqlx::AnyConnection::connect(&db_url).await?;
    let mut db = sqlx::PgConnection::connect(&db_url).await?;
    test_clean(&mut db).await?;
    //let db = sqlx::AnyPool::connect(&db_url).await?;
    //db.ac
    Ok(())
}
