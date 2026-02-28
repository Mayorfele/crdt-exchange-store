use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn};
use crate::writer::write_rate;

// Frankfurter API response shape
#[derive(serde::Deserialize)]
struct FrankfurterResponse {
    base: String,
    rates: HashMap<String, f64>,
}

pub async fn run(
    target_node: String,
    interval_secs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

    // currency pairs we care about
    let pairs = vec!["EUR", "GBP", "JPY", "NGN", "CAD", "AUD"];

    loop {
        interval.tick().await;

        let url = "https://api.frankfurter.app/latest?from=USD";

        match client.get(url).send().await {
            Ok(resp) => {
                match resp.json::<FrankfurterResponse>().await {
                    Ok(data) => {
                        for pair in &pairs {
                            if let Some(rate) = data.rates.get(*pair) {
                                let key = format!("rates:fiat:USD/{}", pair);
                                let value = rate.to_string();

                                info!("frankfurter: {} = {}", key, value);

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
                    Err(e) => warn!("failed to parse frankfurter response: {}", e),
                }
            }
            Err(e) => warn!("failed to fetch from frankfurter: {}", e),
        }
    }
}