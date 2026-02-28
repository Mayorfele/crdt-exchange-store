use std::time::Duration;
use tracing::{info, warn};
use crate::writer::write_rate;

#[derive(serde::Deserialize)]
struct CoinCapAsset {
    id: String,
    #[serde(rename = "priceUsd")]
    price_usd: String,
}

#[derive(serde::Deserialize)]
struct CoinCapResponse {
    data: Vec<CoinCapAsset>,
}

pub async fn run(
    target_node: String,
    interval_secs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

    loop {
        interval.tick().await;

        let url = "https://api.coincap.io/v2/assets?ids=bitcoin,ethereum,solana";

        match client.get(url).send().await {
            Ok(resp) => {
                match resp.json::<CoinCapResponse>().await {
                    Ok(data) => {
                        let coin_map = [
                            ("bitcoin",  "BTC"),
                            ("ethereum", "ETH"),
                            ("solana",   "SOL"),
                        ];

                        for asset in &data.data {
                            if let Some((_, ticker)) = coin_map.iter()
                                .find(|(id, _)| *id == asset.id.as_str())
                            {
                                let key = format!("rates:crypto:{}/USD", ticker);
                                let value = asset.price_usd.clone();

                                info!("coincap: {} = {}", key, value);

                                if let Err(e) = write_rate(
                                    &target_node,
                                    key,
                                    value,
                                ).await {
                                    warn!("failed to write rate: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => warn!("failed to parse coincap response: {}", e),
                }
            }
            Err(e) => warn!("failed to fetch from coincap: {}", e),
        }
    }
}