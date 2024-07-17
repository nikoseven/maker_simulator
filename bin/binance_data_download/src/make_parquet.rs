use std::{
    fs::{create_dir_all, File},
    io::{Cursor, Read},
    path::{Path, PathBuf},
};

use anyhow::Context;
use chrono::NaiveDate;
use polars::io::{
    csv::CsvReader,
    parquet::{ParquetReader, ParquetWriter},
    SerReader,
};

#[derive(Debug)]
struct MakeParquetTask {
    pub csv_zip_path: PathBuf,
    pub parquet_path: PathBuf,
}

impl MakeParquetTask {
    async fn make_parquet(&self) -> Result<(), anyhow::Error> {
        let csv_zip_file = File::open(&self.csv_zip_path)?;
        let mut csv_zip_archive = zip::read::ZipArchive::new(csv_zip_file)?;
        if csv_zip_archive.len() != 1 {
            panic!(
                "zip file should contain only one file, but found {} files, file={:?}",
                csv_zip_archive.len(),
                self.csv_zip_path
            );
        }
        println!("reading zip file: {:?}", self.csv_zip_path);
        let mut csv_content = vec![];
        csv_zip_archive
            .by_index(0)
            .expect("failed to read zip file")
            .read_to_end(&mut csv_content)?;
        let csv_reader = CsvReader::new(Cursor::new(csv_content));
        let mut dataframe = csv_reader.finish()?;
        println!("finished read.");
        println!("writing parquet file: {:?}", self.parquet_path);
        create_dir_all(
            self.parquet_path
                .parent()
                .with_context(|| "File path no parent")?,
        )?;
        let mut parquet_file = std::fs::File::create(&self.parquet_path)?;
        ParquetWriter::new(&mut parquet_file).finish(&mut dataframe)?;
        Ok(())
    }

    fn zip_file_missing_or_corrupted(&self) -> bool {
        let file = std::fs::File::open(&self.csv_zip_path);
        if file.is_err() {
            // file not existed
            return true;
        }
        let file = file.unwrap();
        let zip_arch = zip::ZipArchive::new(file);
        if zip_arch.is_err() {
            // file corrupted
            return true;
        }
        false
    }

    fn parquet_file_missing_or_corrupted(&self) -> bool {
        let file = std::fs::File::open(&self.parquet_path);
        if file.is_err() {
            // file not existed
            return true;
        }
        println!("checking parquet file: {:?}", self.parquet_path);
        let file = file.unwrap();
        let parquet_reader = ParquetReader::new(file);
        if parquet_reader.finish().is_err() {
            // file corrupted
            println!("parquet file corrupted: {:?}", self.parquet_path);
            return true;
        }
        false
    }
}

fn generate_make_parquet_task(
    date_range: &[NaiveDate],
    symbol: &str,
    root_path: &Path,
) -> Vec<MakeParquetTask> {
    date_range
        .iter()
        .flat_map(|date| {
            let date_str = date.format("%Y-%m-%d").to_string();
            [
                MakeParquetTask {
                    csv_zip_path: root_path
                        .join(format!("future_um/{}/trades/{}.zip", symbol, date_str)),
                    parquet_path: root_path.join(format!(
                        "future_um/{}/trades_pq/{}.parquet",
                        symbol, date_str
                    )),
                },
                MakeParquetTask {
                    csv_zip_path: root_path
                        .join(format!("future_um/{}/bookticker/{}.zip", symbol, date_str)),
                    parquet_path: root_path.join(format!(
                        "future_um/{}/bookticker_pq/{}.parquet",
                        symbol, date_str
                    )),
                },
            ]
        })
        .collect()
}

pub async fn process_make_parquet_command(
    date_range: &[NaiveDate],
    symbol: &str,
    root_path: &Path,
    max_task: usize,
) {
    // set env POLARS_MAX_THREADS to max_task if max_task > 0
    if max_task > 0 {
        println!("setting POLARS_MAX_THREADS to {}", max_task);
        std::env::set_var("POLARS_MAX_THREADS", max_task.to_string());
    }

    let tasks = generate_make_parquet_task(date_range, symbol, root_path);
    for task in tasks {
        if !task.parquet_file_missing_or_corrupted() {
            println!("parquet file already existed: {:?}", task.parquet_path);
            continue;
        }
        if task.zip_file_missing_or_corrupted() {
            eprintln!("zip file missing or corrupted: {:?}", task.csv_zip_path);
            continue;
        }
        let _ = task.make_parquet().await.map_err(|err| {
            eprintln!("failed to process task: {:?}, error: {:?}", task, err);
        });
    }
}
