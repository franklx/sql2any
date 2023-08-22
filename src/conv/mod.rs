pub mod json;
pub mod xlsx;
pub mod gfm;

use anyhow::Result;
use sqlx::types::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime};
use sqlx::types::{Decimal, JsonValue};
use sqlx::{mysql::MySqlColumn, postgres::PgColumn, Decode, Type, TypeInfo};
use sqlx::{Column, ColumnIndex, Database};
use std::path::Path;

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
    pub(crate) name: String,
    pub(crate) kind: FieldKind,
}

pub trait Converter<'a, DB> {
    type ConvFn;

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
        usize: ColumnIndex<DB::Row>;

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
        //for<'b> Field: Converter<'b, DB>
        ;
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