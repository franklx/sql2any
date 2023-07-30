use anyhow::Result;
use enum_map::{enum_map, Enum, EnumMap};
use num_traits::ToPrimitive;
use rust_xlsxwriter::{ColNum, Format, RowNum, Workbook, Worksheet};
use sqlx::types::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::types::{Decimal, JsonValue};
use sqlx::{ColumnIndex, Database, Decode, Type, Row};
use std::marker::PhantomData;
use std::path::Path;

use super::{Field, Converter, FieldKind};

#[derive(Enum)]
pub enum XF {
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

type XlsxFmtMap = EnumMap<XF, Format>;
type XlsxConvFn<'a, R> = fn(RowNum, ColNum, &mut Worksheet, &'a R, &XlsxFmtMap) -> Result<()>;

pub struct XLSX<DB: Database> {
    phantom: PhantomData<DB>,
}

impl<'a, DB: Database> Converter<'a, DB> for XLSX<DB> {
    type ConvFn = XlsxConvFn<'a, DB::Row>;

    fn convert(field: &Field) -> Self::ConvFn
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
        match field.kind {
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

    fn write(result: &[DB::Row], output: impl AsRef<Path>) -> Result<()>
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
        if !result.is_empty() {
            let r = 0;
            let columns: Vec<Field> = result[0].columns().iter().map(|c| c.into()).collect();
            let convs = columns
                .iter()
                .enumerate()
                .inspect(|(c, fld)| {
                    ws.write_with_format(r, *c as ColNum, &fld.name, &xf[XF::Bold]).unwrap();
                })
                .map(|(_c, fld)| Self::convert(fld))
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
}
