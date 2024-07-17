use binance_republisher::binance_republisher::BinanceRepublisherBuilder;
use clap::Parser;
use market_agent::market_agent::MarketAgentBuilder;
use mimalloc::MiMalloc;
use simulation::engine::SimulationEngineBuilder;
use std::path::PathBuf;
use stepper::stepper::StepperBuilder;
use symbol_info::SymbolInfoManager;
use tracing::info;
use vis::vis_module::VisModuleBuilder;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(version, about = "Upstair simulation", long_about = None)]
struct CliArgs {
    #[clap(long, short = 'p')]
    path: Vec<PathBuf>,

    #[clap(long, default_value = "BTCUSDT")]
    symbol: Option<String>,

    #[clap(long, short = 'v', default_value_t = tracing::Level::ERROR)]
    log_level: tracing::Level,

    #[clap(long, action)]
    no_progress: bool,

    #[clap(long, short = 'g', action)]
    vis: bool,

    #[clap(long, short = 'd')]
    date: Option<String>,

    #[clap(long, short = 'r', default_value = "data/future_um")]
    root_path: PathBuf,
}

fn main() {
    let cli = CliArgs::parse();
    println!("{:?}", cli);

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(cli.log_level)
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // Init symbol
    let symbol_info_manager = SymbolInfoManager::default()
        .with_symbol_config("BTCUSDT", "BTC", "USDT", /*fee rate*/ 0.0000);
    let symbol: String = cli.symbol.expect("symbol is not provided");
    let symbol: &'static str = symbol.leak();
    // TODO: a better way to determine base asset and quote asset
    let base_asset = &symbol[0..symbol.len() - 4];
    let quote_asset = &symbol[symbol.len() - 4..];

    let mut engine = SimulationEngineBuilder::default()
        .add_module(
            StepperBuilder::new(symbol).with_symbol_info_manager(symbol_info_manager.clone()),
        )
        .add_module(
            MarketAgentBuilder::default()
                .with_symbol_info_manager(symbol_info_manager.clone())
                .with_initial_balance(quote_asset, 50000.0)
                .with_initial_balance(base_asset, 1.0),
        );

    let republish_path = {
        if cli.path.is_empty() {
            let date = cli.date.as_ref().unwrap();
            vec![
                cli.root_path
                    .join(symbol)
                    .join("trades")
                    .join(format!("{date}.zip")),
                cli.root_path
                    .join(symbol)
                    .join("bookticker")
                    .join(format!("{date}.zip")),
            ]
        } else {
            cli.path
        }
    };
    println!("Republish data path: {:?}", republish_path);

    if !republish_path.is_empty() {
        let republisher =
            BinanceRepublisherBuilder::new(symbol).set_show_progress(!cli.no_progress);
        let republisher = republish_path.iter().fold(republisher, |b, path| {
            b.with_file(path.to_str().unwrap())
                .unwrap_or_else(|_| panic!("failed to open {}", path.to_str().unwrap()))
        });
        engine = engine.add_module(republisher);
    } else {
        panic!("path is not provided");
    }

    if cli.vis {
        engine = engine.add_module(
            VisModuleBuilder::default()
                .with_symbol_info_manager(symbol_info_manager.clone())
                .with_initial_balance(quote_asset, 50000.0)
                .with_initial_balance(base_asset, 1.0),
        );
    }

    let mut engine = engine.build();
    info!("engine start");
    engine.run();
}
