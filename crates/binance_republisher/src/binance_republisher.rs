use anyhow::Context;
use std::{
    ffi::OsStr,
    fs::File,
    io::{BufRead, BufReader},
    iter::Peekable,
    path::{Path, PathBuf},
    sync::mpsc::{self, sync_channel, Receiver},
    thread,
    time::{Duration, UNIX_EPOCH},
};

use upstair_type::{
    data::market::{BinanceBookTicker, BinanceTradeTick},
    module::{Module, ModuleBuilder, WriteTopicHandle},
    Message, Payload,
};

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use tracing::info;

#[derive(Debug, Default)]
enum PeekingTick {
    #[default]
    None,
    TradeTick(BinanceTradeTick),
    BookTicker(BinanceBookTicker),
}

pub struct BinanceRepublisher {
    write_market_data_handle: WriteTopicHandle,
    trade_tick_peekable_iter: Peekable<mpsc::IntoIter<BinanceTradeTick>>,
    bookticker_peekable_iter: Peekable<mpsc::IntoIter<BinanceBookTicker>>,
    peeking_tick: PeekingTick,
    peeking_tick_time: std::time::SystemTime,
}

impl Module for BinanceRepublisher {
    fn sync(&mut self, _: &mut dyn upstair_type::module::ModuleComms) -> bool {
        true
    }

    fn one_iteration(&mut self, comms: &mut dyn upstair_type::module::ModuleComms) {
        let now = comms.time();
        loop {
            if self.peeking_tick_time > now {
                break;
            }
            let payload = match std::mem::take(&mut self.peeking_tick) {
                PeekingTick::TradeTick(tick) => Payload::BinanceTradeTick(tick),
                PeekingTick::BookTicker(tick) => Payload::BinanceBookTicker(tick),
                PeekingTick::None => break,
            };
            comms.publish(
                &self.write_market_data_handle,
                Message {
                    header: upstair_type::MessageHeader {
                        commit_at: self.peeking_tick_time,
                    },
                    payload,
                },
            );
            self.next_tick();
            if matches!(self.peeking_tick, PeekingTick::None) {
                comms.request_terminate();
                return;
            }
        }
    }

    fn next_iteration_start_at(&self) -> Option<std::time::SystemTime> {
        match self.peeking_tick {
            PeekingTick::None => None,
            _ => Some(self.peeking_tick_time),
        }
    }

    fn start(&mut self) {
        self.next_tick();
    }

    fn wake_on_message(&self) -> bool {
        false
    }
}

impl BinanceRepublisher {
    fn next_tick(&mut self) -> bool {
        match (
            self.trade_tick_peekable_iter.peek(),
            self.bookticker_peekable_iter.peek(),
        ) {
            (None, None) => {
                info!("no more tick to read");
                self.peeking_tick = PeekingTick::None;
                false
            }
            (None, Some(bookticker)) => {
                self.peeking_tick = PeekingTick::BookTicker(bookticker.clone());
                self.peeking_tick_time = UNIX_EPOCH + Duration::from_millis(bookticker.event_time);
                self.bookticker_peekable_iter.next();
                true
            }
            (Some(trade_tick), None) => {
                self.peeking_tick = PeekingTick::TradeTick(trade_tick.clone());
                self.peeking_tick_time = UNIX_EPOCH + Duration::from_millis(trade_tick.time);
                self.trade_tick_peekable_iter.next();
                true
            }
            (Some(trade_tick), Some(bookticker)) => {
                if trade_tick.time < bookticker.event_time {
                    self.peeking_tick = PeekingTick::TradeTick(trade_tick.clone());
                    self.peeking_tick_time = UNIX_EPOCH + Duration::from_millis(trade_tick.time);
                    self.trade_tick_peekable_iter.next();
                } else {
                    self.peeking_tick = PeekingTick::BookTicker(bookticker.clone());
                    self.peeking_tick_time =
                        UNIX_EPOCH + Duration::from_millis(bookticker.event_time);
                    self.bookticker_peekable_iter.next();
                }
                true
            }
        }
    }
}

pub struct BinanceRepublisherBuilder {
    symbol: &'static str,
    write_target_topic_handle: Option<WriteTopicHandle>,
    files: Vec<(File, PathBuf)>,
    show_progress: bool,
}

impl BinanceRepublisherBuilder {
    pub fn new(symbol: &'static str) -> Self {
        BinanceRepublisherBuilder {
            symbol,
            write_target_topic_handle: None,
            files: vec![],
            show_progress: false,
        }
    }

    pub fn with_file(mut self, path: &str) -> Result<Self, anyhow::Error> {
        let file = File::open(path).with_context(|| format!("failed to open {}", &path))?;
        self.files.push((file, path.into()));
        Ok(self)
    }

    pub fn set_show_progress(mut self, show_progress: bool) -> Self {
        self.show_progress = show_progress;
        self
    }
}

impl ModuleBuilder for BinanceRepublisherBuilder {
    fn name(&self) -> &str {
        "binance_republisher"
    }

    fn init_comm(&mut self, comms: &mut dyn upstair_type::module::ModuleCommsBuilder) {
        let target_topic = comms.get_topic("market_data");
        self.write_target_topic_handle = comms.publish_topic(&target_topic).into();
    }

    fn build(self: Box<BinanceRepublisherBuilder>) -> Box<dyn Module> {
        let write_target_topic_handle = self.write_target_topic_handle.clone().unwrap();
        let files = self.files;
        let (trade_tick_files, files): (Vec<_>, Vec<_>) = files
            .into_iter()
            .partition(|(_, path)| BinanceTradeTick::file_name_matched(path));
        let tick_rx = Self::spawn_csv_reader::<BinanceTradeTick>(
            trade_tick_files,
            self.symbol,
            self.show_progress,
        );
        let (bookticker_files, _): (Vec<_>, Vec<_>) = files
            .into_iter()
            .partition(|(_, path)| BinanceBookTicker::file_name_matched(path));
        let bookticker_rx =
            Self::spawn_csv_reader::<BinanceBookTicker>(bookticker_files, self.symbol, false);
        Box::new(BinanceRepublisher {
            write_market_data_handle: write_target_topic_handle,
            peeking_tick_time: std::time::SystemTime::UNIX_EPOCH, // this will be set in start when buffering data
            trade_tick_peekable_iter: tick_rx.into_iter().peekable(),
            bookticker_peekable_iter: bookticker_rx.into_iter().peekable(),
            peeking_tick: PeekingTick::None,
        })
    }
}

impl BinanceRepublisherBuilder {
    fn spawn_csv_reader<T: ParseFromCsvFile + Send + 'static>(
        files: Vec<(File, PathBuf)>,
        symbol: &'static str,
        show_progress: bool,
    ) -> Receiver<T> {
        let (tx, rx) = sync_channel(1024);
        thread::spawn(move || {
            files.iter().for_each(|(file, file_path_buf)| {
                // setup progress bar
                let progress_bar = ProgressBar::new(file.metadata().unwrap().len());
                progress_bar.set_style(
                    ProgressStyle::default_bar()
                        .template("{spinner:.green} [{elapsed}] (eta: {eta}) [{bar:40.cyan/blue}] {bytes}/{total_bytes} at {bytes_per_sec} : {msg}")
                        .unwrap()
                        .progress_chars("##-"),
                );
                if show_progress {
                    // set message to file name
                    progress_bar.set_message(format!(
                        "republish {}",
                        file_path_buf
                            .file_name()
                            .unwrap_or_default()
                            .to_str()
                            .unwrap_or_default()
                    ));
                } else {
                    progress_bar.set_draw_target(ProgressDrawTarget::hidden());
                }
                let file = progress_bar.wrap_read(file);
                let is_zip = file_path_buf.extension().map_or(false, |ext| ext == "zip");
                if is_zip {
                    let mut zip_file = zip::read::ZipArchive::new(file).unwrap_or_else(|e| panic!("failed to open zip file {:?}. error={:?}", file_path_buf, e));
                    if zip_file.len() != 1 {
                        panic!("zip file should contain only one file, but found {} files, file={:?}", zip_file.len(), file_path_buf);
                    }
                    let csv_file = zip_file.by_index(0).expect("failed to read zip file");
                    let lines = BufReader::new(csv_file).lines();
                    for l in lines {
                        match l {
                            Ok(line) =>  {
                                let parsed = T::parse_csv_line(&line, symbol);
                                if parsed.is_ok() && tx.send(parsed.unwrap()).is_err() {
                                // channel closed stop reading
                                    return;
                                }
                            },
                            Err(_) => continue,
                        }
                    }
                }
                else {
                    let lines = BufReader::new(file).lines();
                    for l in lines {
                        match l {
                            Ok(line) =>  {
                                let parsed = T::parse_csv_line(&line, symbol);
                                if parsed.is_ok() && tx.send(parsed.unwrap()).is_err() {
                                // channel closed stop reading
                                    return;
                                }
                            },
                            Err(_) => continue,
                        }
                    }
                }
            })
        });
        rx
    }
}

fn make_binance_tick(
    id: Option<&str>,
    price: Option<&str>,
    qty: Option<&str>,
    base_qty: Option<&str>,
    time: Option<&str>,
    is_buyer_maker: Option<&str>,
    symbol: &'static str,
) -> Result<BinanceTradeTick, anyhow::Error> {
    // parse bool
    Ok(BinanceTradeTick {
        id: id.unwrap().parse().with_context(|| "failed to parse id")?,
        price: price
            .with_context(|| "no price")?
            .parse()
            .with_context(|| "failed to parse price")?,
        qty: qty
            .with_context(|| "no qty")?
            .parse()
            .with_context(|| "failed to parse qty")?,
        base_qty: base_qty
            .with_context(|| "no base_qty")?
            .parse()
            .with_context(|| "failed to parse base_qty")?,
        time: time
            .with_context(|| "no time")?
            .parse()
            .with_context(|| "failed to parse base_qty")?,
        is_buyer_maker: is_buyer_maker
            .with_context(|| "no is_buyer_maker")?
            .to_lowercase()
            == "true",
        symbol,
    })
}

trait ParseFromCsvFile: Sized {
    fn parse_csv_line(s: &str, symbol: &'static str) -> Result<Self, anyhow::Error>;
    fn file_name_matched(pathbuf: &Path) -> bool;
}

impl ParseFromCsvFile for BinanceTradeTick {
    fn parse_csv_line(s: &str, symbol: &'static str) -> Result<Self, anyhow::Error> {
        let mut fields = s.split(',');
        let id = fields.next();
        let price = fields.next();
        let qty = fields.next();
        let base_qty = fields.next();
        let time = fields.next();
        let is_buyer_maker = fields.next();
        make_binance_tick(id, price, qty, base_qty, time, is_buyer_maker, symbol)
    }

    fn file_name_matched(pathbuf: &Path) -> bool {
        if pathbuf.extension() == Some(OsStr::new("zip")) {
            pathbuf.to_str().unwrap().contains("trades")
        } else {
            pathbuf
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .contains("trades")
        }
    }
}

impl ParseFromCsvFile for BinanceBookTicker {
    fn parse_csv_line(s: &str, symbol: &'static str) -> Result<Self, anyhow::Error> {
        let mut fields = s.split(',');
        let update_id = fields
            .next()
            .with_context(|| "failed to parse update_id")?
            .parse()?;
        let best_bid_price = fields
            .next()
            .with_context(|| "failed to parse best_bid_price")?
            .parse()?;
        let best_bid_qty = fields
            .next()
            .with_context(|| "failed to parse best_bid_qty")?
            .parse()?;
        let best_ask_price = fields
            .next()
            .with_context(|| "failed to parse best_ask_price")?
            .parse()?;
        let best_ask_qty = fields
            .next()
            .with_context(|| "failed to parse best_ask_qty")?
            .parse()?;
        let transaction_time = fields
            .next()
            .with_context(|| "failed to parse transaction_time")?
            .parse()?;
        let event_time = fields
            .next()
            .with_context(|| "failed to parse event_time")?
            .parse()?;

        Ok(BinanceBookTicker {
            update_id,
            best_bid_price,
            best_bid_qty,
            best_ask_price,
            best_ask_qty,
            transaction_time,
            event_time,
            symbol,
        })
    }

    fn file_name_matched(pathbuf: &Path) -> bool {
        if pathbuf.extension() == Some(OsStr::new("zip")) {
            pathbuf.to_str().unwrap().contains("bookticker")
        } else {
            pathbuf
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .contains("bookTicker")
        }
    }
}
