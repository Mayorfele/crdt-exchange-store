mod frankfurter;
mod coingecko;
mod writer;

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "feed-ingester")]
pub struct Config {
    // which source to pull from
    #[arg(long, env = "SOURCE")]
    pub source: String,

    // which node to write to
    #[arg(long, env = "TARGET_NODE", default_value = "http://localhost:7001")]
    pub target_node: String,

    // how often to fetch in seconds
    #[arg(long, env = "FETCH_INTERVAL", default_value = "5")]
    pub fetch_interval: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("info".parse()?))
        .init();

    let config = Config::parse();
    info!("starting ingester — source: {} → {}", config.source, config.target_node);

    match config.source.as_str() {
        "frankfurter" => {
            frankfurter::run(config.target_node, config.fetch_interval).await?;
        }
        "coingecko" => {
            coingecko::run(config.target_node, config.fetch_interval).await?;
        }
        other => {
            eprintln!("unknown source: {}. use 'frankfurter' or 'coingecko'", other);
            std::process::exit(1);
        }
    }

    Ok(())
}