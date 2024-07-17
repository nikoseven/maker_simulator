use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use chrono::NaiveDate;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
use tokio::{fs::File, io::AsyncWriteExt, sync::Semaphore};

use crate::get_url::{self, BinanceBizType, DataProductName};

#[derive(Debug)]
pub struct DownloadTask {
    pub uri: String,
    pub path: PathBuf,
}

impl DownloadTask {
    pub async fn download(&self, mp: &MultiProgress) -> Result<(), anyhow::Error> {
        let client = Client::new();
        let mut rsp = client.get(&self.uri).send().await?;
        let total_size = rsp.content_length().with_context(|| "No content length")?;
        let pb = mp.add(ProgressBar::new(total_size));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed:4}] (eta: {eta:4}) [{bar:40.cyan/blue}] {bytes}/{total_bytes} at {bytes_per_sec} : {msg}")
                .unwrap()
                .progress_chars("##-"));

        let path_str = self.path.to_str().unwrap_or("-");
        pb.set_message(format!("Downloading to {}", path_str));
        create_dir_all(self.path.parent().with_context(|| "File path no parent")?)?;
        let mut file = File::create(&self.path).await?;
        let mut downloaded: u64 = 0;
        while let Some(chunk) = rsp.chunk().await.unwrap() {
            file.write_all(&chunk).await.unwrap();
            downloaded += chunk.len() as u64;
            pb.set_position(downloaded);
        }

        pb.finish_with_message(format!("Finished {}", path_str));
        mp.remove(&pb);

        Ok(())
    }

    pub fn need_download(&self) -> bool {
        let file = std::fs::File::open(&self.path);
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
}

fn generate_future_download_tasks(
    date_range: &[NaiveDate],
    symbol: &str,
    root_path: &Path,
) -> Vec<DownloadTask> {
    date_range
        .iter()
        .flat_map(|date| {
            let date_str = date.format("%Y-%m-%d").to_string();
            let trade_url = get_url::get_data_url(
                symbol,
                BinanceBizType::FutureUm,
                DataProductName::Trades,
                &date_str,
            );
            let bookticker_url = get_url::get_data_url(
                symbol,
                BinanceBizType::FutureUm,
                DataProductName::BookTicker,
                &date_str,
            );
            println!("trade_url: {}", trade_url);
            println!("bookticker_url: {}", bookticker_url);
            [
                DownloadTask {
                    uri: trade_url,
                    path: root_path.join(format!("future_um/{}/trades/{}.zip", symbol, date_str)),
                },
                DownloadTask {
                    uri: bookticker_url,
                    path: root_path
                        .join(format!("future_um/{}/bookticker/{}.zip", symbol, date_str)),
                },
            ]
        })
        .collect()
}

pub async fn process_download_command(
    date_range: &[NaiveDate],
    symbol: &str,
    root_path: &Path,
    max_task: usize,
) {
    let mp = Arc::new(MultiProgress::new());
    let max_task_semaphore = Arc::new(Semaphore::new(max_task));
    let tasks = generate_future_download_tasks(date_range, symbol, root_path);
    let handles = tasks
        .into_iter()
        .filter(|task| task.need_download())
        .map(|task| {
            let mp = mp.clone();
            let sem = max_task_semaphore.clone();
            tokio::spawn(async move {
                let _ = sem.acquire().await.unwrap();
                let result = task.download(mp.as_ref()).await;
                if let Err(e) = result {
                    eprintln!("DownloadTaskFailed error={:?} task={:?}", e, task);
                }
            })
        })
        .collect::<Vec<_>>();
    for h in handles {
        h.await.unwrap();
    }
}
