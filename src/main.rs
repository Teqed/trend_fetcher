// src/main.rs

use mastodon_async::prelude::*;
use mastodon_async::helpers::toml; // requires `features = ["toml"]`
use mastodon_async::{helpers::cli, Result};

#[tokio::main]
async fn main() -> Result<()> {
    const HOME_SERVER: &str = "https://mastodon.shatteredsky.net";
    let server_tokens: Vec<_> = std::fs::read_dir("federation")?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_file() {
                let file_name = path.file_name()?.to_str()?;
                if file_name.ends_with("-data.toml") {
                    Some(file_name.trim_end_matches("-data.toml").to_string())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    // Print each server and its token.
    for server in server_tokens {
        println!("{}: {}", server, server);
    }

    let home = Fed::register(HOME_SERVER).await?;
    let remote: Mastodon = Fed::register("https://mastodon.social").await?;

    Fed::me(&home).await?;
    Fed::me(&remote).await?;

    // Get the trending posts from the remote server.
    let trends: Vec<Status> = Fed::fetch_trending_statuses(&remote).await?;
    // Ask the home server to 'search' for each trend.
    for trend in trends {
        let search = home.search(&trend.uri, true).await?;
        println!("{}: {}", trend.uri, search.accounts.len());
    }
    

    Ok(())
}

pub struct Fed;

impl Fed {
    pub async fn register(server: &str) -> Result<Mastodon> {
        let base = server.replace("https://", "");
        if let Ok(data) = toml::from_file(format!("federation/{}-data.toml", base)) {
            return Ok(Mastodon::from(data));
        }
        let registration = Registration::new(server)
                                        .client_name("mastodon-async-examples")
                                        .build()
                                        .await?;
        let mastodon = cli::authenticate(registration).await?;
        toml::to_file(&mastodon.data, format!("federation/{}-data.toml", base))?;
        Ok(mastodon)
    }

    pub async fn me(mastodon: &Mastodon) -> Result<()> {
        let me = mastodon.verify_credentials().await?;
        println!("You are logged in as: {}", me.acct);
        Ok(())
    }

    pub async fn fetch_trending_statuses(mastodon: &Mastodon) -> Result<Vec<Status>> {
        let url = format!("{}/api/v1/trends/statuses", mastodon.data.base);
        let fetched_json = reqwest::get(&url).await?.text().await?;
        let trends: Vec<Status> = serde_json::from_str(&fetched_json)?;
        Ok(trends)
    }
}