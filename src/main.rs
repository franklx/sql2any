use anyhow::Result;
use clap::Parser;
use conv::json::JSON;
use conv::xlsx::XLSX;
use conv::gfm::GFM;
use conv::Converter;
use sqlx::{Connection, Database, Executor, IntoArguments, MySql, Postgres};
use std::env::var;
use std::path::{Path, PathBuf};
use url::Url;

pub mod conv;

#[derive(Parser, Debug)]
struct Args {
    /// Database url to connect to
    #[arg(short, long)]
    url: Option<String>,

    /// Output filename
    #[arg(short, long)]
    output: PathBuf,

    /// SQL query to execute
    #[arg()]
    query: String,
}

async fn db_fetch<'a, DB>(db_url: &'a Url, sql: &'a str) -> Result<Vec<DB::Row>>
where
    DB: Database,
    DB::Arguments<'a>: IntoArguments<'a, DB>,
    for<'b> &'b mut DB::Connection: Executor<'b, Database = DB>,
{
    let mut db = DB::Connection::connect(db_url.as_str()).await?;
    let result = sqlx::query(sql).fetch_all(&mut db).await?;
    Ok(result)
}

// Thanks to DanielKeep
// https://users.rust-lang.org/t/macro-generating-complete-match/97827
macro_rules! matcher {
    ($params:ident : $($str1:literal => $typ1:ty),* ; $($str2:literal => $typ2:ident),* ;) => {
        matcher! {
            @step
            $params;
            { };
            $($str1 => $typ1),*;
            $($str2 => $typ2),*;
        }
    };

    (@step
        $params:ident;
        { $($arms:tt)* };
        $str1_head:literal => $typ1_head:ty
        $(, $str1_tail:literal => $typ1_tail:ty)*;
        $($str2:literal => $typ2:ident),*;
    ) => {
        matcher! {
            @step
            $params;
            {
                $($arms)*
                $(
                    ($str1_head, $str2) => {
                        $typ2::<$typ1_head>::write(&db_fetch::<$typ1_head>($params.db_url, $params.query).await?, $params.output)?;
                    }
                )*
            };
            $($str1_tail => $typ1_tail),*;
            $($str2 => $typ2),*;
        }
    };

    (@step
        $params:ident;
        { $($arms:tt)* };
        ;
        $($tail:tt)*
    ) => {
        match ($params.db_url.scheme(), $params.format) {
            $($arms)*
            _ => (),
        }
    };
}

struct Params<'a> {
    db_url: &'a Url,
    query: &'a str,
    output: &'a Path,
    format: &'a str,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let params = Params {
        db_url: &Url::parse(
            args.url
                .unwrap_or_else(|| var("DATABASE_URL").expect("DATABASE_URL must be set if url not provided"))
                .as_ref(),
        )
        .expect("Invalid url"),
        query: &args.query,
        output: &args.output,
        format: args.output.extension().unwrap().to_str().unwrap(),
    };

    matcher!(
        params
        :
        "mysql" => MySql,
        "postgres" => Postgres
        ;
        "json" => JSON,
        "xlsx" => XLSX,
        "gfm" => GFM
        ;
    );

    Ok(())
}
