mod client;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "kvstore-cli")]
pub struct Cli {
    // which node to talk to
    #[arg(long, env = "NODE_ADDR", default_value = "http://localhost:7001")]
    pub node: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    // GET a value by key
    Get {
        key: String,
    },
    // SET a key to a value
    Set {
        key: String,
        value: String,
    },
    // DELETE a key
    Delete {
        key: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env()
            .add_directive("warn".parse()?))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Get { key } => {
            client::get(&cli.node, key).await?;
        }
        Command::Set { key, value } => {
            client::set(&cli.node, key, value).await?;
        }
        Command::Delete { key } => {
            client::delete(&cli.node, key).await?;
        }
    }

    Ok(())
}