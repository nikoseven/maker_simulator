mod download_task;
mod get_url;
mod make_parquet;
use chrono::NaiveDate;
use clap::{Parser, Subcommand};
pub use download_task::*;
use make_parquet::process_make_parquet_command;
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct BinanceDownloadCliArgs {
    #[clap(long, short = 'p', default_value = "data")]
    path: PathBuf,

    #[clap(long, short = 's', default_value = "BTCUSDT")]
    symbol: String,

    #[clap(long, short = 'a')]
    start_date: String,

    #[clap(long, short = 'b')]
    end_date: String,

    #[clap(long, short = 'm', default_value = "3")]
    max_task: usize,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Download {},
    MakeParquet {},
}

#[tokio::main]
async fn main() {
    let cli = BinanceDownloadCliArgs::parse();

    let start_date = {
        let d = NaiveDate::parse_from_str(&cli.start_date, "%Y%m%d");
        if d.is_err() {
            panic!("Invalid start date");
        }
        d.unwrap()
    };

    let end_date = {
        let d = NaiveDate::parse_from_str(&cli.end_date, "%Y%m%d");
        if d.is_err() {
            panic!("Invalid end date");
        }
        d.unwrap()
    };

    let date_range = {
        let mut dates = vec![];
        let mut date = start_date;
        while date <= end_date {
            dates.push(date);
            date = date.succ_opt().unwrap();
        }
        dates
    };

    match cli.command {
        Commands::Download {} => {
            process_download_command(&date_range, &cli.symbol, &cli.path, cli.max_task).await
        }
        Commands::MakeParquet {} => {
            process_make_parquet_command(&date_range, &cli.symbol, &cli.path, cli.max_task).await
        }
    }
}
