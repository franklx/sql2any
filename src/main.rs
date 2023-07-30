use anyhow::Result;
use clap::Parser;
use conv::Converter;
use conv::json::JSON;
use conv::xlsx::XLSX;
use sqlx::database::HasArguments;
use sqlx::{Connection, Database, Executor, IntoArguments, MySql, Postgres};
use std::env::var;
use url::Url;

pub mod conv;

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

async fn db_fetch<'a, DB>(db_url: &'a Url, sql: &'a str) -> Result<Vec<DB::Row>>
where
    DB: Database,
    <DB as HasArguments<'a>>::Arguments: IntoArguments<'a, DB>,
    for<'b> &'b mut DB::Connection: Executor<'b, Database = DB>,
{
    let mut db = DB::Connection::connect(db_url.as_str()).await?;
    let result = sqlx::query(sql).fetch_all(&mut db).await?;
    Ok(result)
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
            let result = db_fetch::<Postgres>(&db_url, &args.query).await?;
            JSON::<Postgres>::write(&result, &args.output)?;
            XLSX::<Postgres>::write(&result, &args.output)?;
        }
        "mysql" => {
            let result = db_fetch::<MySql>(&db_url, &args.query).await?;
            JSON::<MySql>::write(&result, &args.output)?;
            XLSX::<MySql>::write(&result, &args.output)?;
        }
        scheme => panic!("Unknown driver {scheme}"),
    }
    Ok(())
}
