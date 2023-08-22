use anyhow::Result;
use enum_map::Enum;
use sqlx::types::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::types::{Decimal, JsonValue};
use sqlx::{ColumnIndex, Database, Decode, Row, Type};
use std::fs::File;
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;

use super::{Converter, Field, FieldKind};

#[derive(Enum)]
pub enum MF {
    Left,
    Center,
    Right,
}

#[macro_export]
macro_rules! gfm_write {
    ($ty:ty) => {
        |c, rw| rw.get::<$ty, _>(c).to_string()
    };
}

#[macro_export]
macro_rules! gfm_write_date {
    ($ty:ty) => {
        |c, rw| rw.get::<$ty, _>(c).unwrap_or_default().to_string()
    };
}

type GfmConvFn<'a, R> = fn(usize, &'a R) -> String;

pub struct GFM<DB: Database> {
    phantom: PhantomData<DB>,
}

impl<'a, DB: Database> Converter<'a, DB> for GFM<DB> {
    type ConvFn = GfmConvFn<'a, DB::Row>;

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
            FieldKind::INT8 => gfm_write!(i8),
            FieldKind::INT16 => gfm_write!(i16),
            FieldKind::INT32 => gfm_write!(i32),
            FieldKind::INT64 => gfm_write!(i64),
            FieldKind::UINT8 => todo!(),
            FieldKind::UINT16 => todo!(),
            FieldKind::UINT32 => todo!(),
            FieldKind::UINT64 => todo!(),
            FieldKind::FLOAT32 => gfm_write!(f32),
            FieldKind::FLOAT64 => gfm_write!(f64),
            FieldKind::STR => gfm_write!(&str),
            FieldKind::BOOL => gfm_write!(bool),
            FieldKind::DECIMAL => gfm_write!(Decimal),
            FieldKind::DATE => gfm_write_date!(Option<NaiveDate>),
            FieldKind::TIME => gfm_write_date!(Option<NaiveTime>),
            FieldKind::DATETIME => gfm_write_date!(Option<NaiveDateTime>),
            FieldKind::DATETIMETZ => gfm_write_date!(Option<DateTime<Local>>),
            FieldKind::JSON => gfm_write!(JsonValue),
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
        if !result.is_empty() {
            let columns: Vec<Field> = result[0].columns().iter().map(|c| c.into()).collect();
            let convs = columns.iter().enumerate().map(|(_c, fld)| Self::convert(fld)).collect::<Vec<_>>();
            let head: Vec<String> = columns.iter().map(|fld| fld.name.clone()).collect();
            let mut body: Vec<Vec<String>> =
                result
                    .iter()
                    .map(|rw|
                        convs
                            .iter()
                            .enumerate()
                            .map(|(c, conv)| conv(c, rw)
                        ).collect()
                    ).collect();
            let lens = body
                .iter()
                .fold(head.iter().map(|c| c.len()).collect::<Vec<_>>(), |mut acc, rw| {
                    acc.iter_mut().zip(rw.iter()).for_each(|(lft, rgt)| {
                        *lft = rgt.len().max(*lft);
                    });
                    acc
                });
            let mut jf = File::create(output)?;

            body.insert(0, head);

            body.insert(1, lens.iter().map(|len| { "-".repeat(*len) }).collect::<Vec<_>>());

            for row in body {
                writeln!(jf, "|{}|", row
                    .iter()
                    .zip(lens.iter())
                    .map(|(fld, len)| {
                        format!(" {fld:<len$} ")
                    }
                ).collect::<Vec<_>>().join("|"))?
            }

        }
        Ok(())
    }
}
