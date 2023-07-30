use anyhow::Result;
use num_traits::ToPrimitive;
use serde_json::{Map, Value};
use sqlx::types::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::types::{Decimal, JsonValue};
use sqlx::{ColumnIndex, Database, Decode, Type, Row};
use std::fs::File;
use std::marker::PhantomData;
use std::path::Path;
use std::io::Write;

use super::{Field, Converter, FieldKind};

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

type JsonMap = Map<String, Value>;
type JsonConvFn<'a, R> = fn(usize, &'a R) -> Value;

pub struct JSON<DB: Database> {
    phantom: PhantomData<DB>,
}

impl<'a, DB: Database> Converter<'a, DB> for JSON<DB> {
    type ConvFn = JsonConvFn<'a, DB::Row>;

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
            FieldKind::DECIMAL => |c, rw| rw.get::<Decimal, _>(c).to_f64().unwrap().into(),
            FieldKind::DATE => json_write_date!(Option<NaiveDate>),
            FieldKind::TIME => json_write_date!(Option<NaiveTime>),
            FieldKind::DATETIME => json_write_date!(Option<NaiveDateTime>),
            FieldKind::DATETIMETZ => json_write_date!(Option<DateTime<Local>>),
            FieldKind::JSON => json_write!(JsonValue),
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
        let mut jf = File::create(output)?;
        writeln!(jf, "[")?;
        if !result.is_empty() {
            let columns: Vec<Field> = result[0].columns().iter().map(|c| c.into()).collect();
            let convs = columns.iter().enumerate().map(|(_c, fld)| Self::convert(fld)).collect::<Vec<_>>();
            for rw in result.iter() {
                let ji = convs
                    .iter()
                    .enumerate()
                    .map(|(c, conv)| (columns[c].name.clone(), conv(c, rw))
                    );
                let jr = JsonMap::from_iter(ji);
                serde_json::to_writer(&jf, &jr)?;
                writeln!(jf, ",")?;
            }
        }
        writeln!(jf, "]")?;
        Ok(())
    }
}
