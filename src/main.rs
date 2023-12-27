// src/main.rs

use std::collections::HashMap;

use mastodon_async::prelude::*;
use mastodon_async::helpers::toml; // requires `features = ["toml"]`
use mastodon_async::{helpers::cli, Result};

const ONE_PAGE: usize = 40;

#[tokio::main]
async fn main() -> Result<()> {
    const HOME_SERVER: &str = "mastodon.shatteredsky.net";
    let mut instance_collection: HashMap<String, Mastodon> = HashMap::new();

    let home = Fed::get_instance(&mut instance_collection, HOME_SERVER).await?;
    let authenticated_strings = [];
    let unauthenticated_strings = [];
    let authenticated_servers: Vec<_> = authenticated_strings.iter().map(|s| async {
        Fed::register(s).await
    }).collect();
    let mut remotes: Vec<Mastodon> = Vec::new();
    for remote in authenticated_servers {
        remotes.push(remote.await?);
    }

    let mut queued_servers: Vec<_> = Vec::new();
    
    let mut statuses = HashMap::new();
    for remote in remotes {
        for status in Fed::fetch_trending_statuses(&remote.data.base, ONE_PAGE).await? {
            // Check if this status is from the server we found it on, by comparing the
            // status URI to the server's base URL
            if status.uri.contains(&remote.data.base as &str) {
                println!("Status: {}", status.uri);
            } else {
                println!("Status from another server: {}", status.uri);
                // Extract the base server URL from the status URI
                let base = status.uri.split('/').nth(2).unwrap();
                // Add the server to the list of servers to fetch statuses from
                println!("Queued server: {}", base);
                queued_servers.push(base.to_string());
            }
            statuses.entry(status.uri.clone()).and_modify(|existing_status: &mut Status| {
                println!("Duplicate status, Reb: {:?}, Rep: {:?}, Fav: {:?}", existing_status.reblogs_count, existing_status.replies_count.unwrap_or(0), existing_status.favourites_count);
                // Use the highest boost count, reply count, and favourite count
                existing_status.reblogs_count = std::cmp::max(existing_status.reblogs_count, status.reblogs_count);
                existing_status.replies_count = std::cmp::max(existing_status.replies_count, status.replies_count);
                existing_status.favourites_count = std::cmp::max(existing_status.favourites_count, status.favourites_count);
            }).or_insert(status);
        }
    }
    println!("Total statuses: {}", statuses.len());
    println!("Queued servers: {}", queued_servers.len());
    // Fetch statuses from the queued servers
    for server in queued_servers {
        // let remote = Fed::register(&server).await?;
        for status in Fed::fetch_trending_statuses(&server, ONE_PAGE).await? {
            statuses.entry(status.uri.clone()).and_modify(|existing_status: &mut Status| {
                println!("Duplicate status, Reb: {:?}, Rep: {:?}, Fav: {:?}", existing_status.reblogs_count, existing_status.replies_count.unwrap_or(0), existing_status.favourites_count);
                // Use the highest boost count, reply count, and favourite count
                existing_status.reblogs_count = std::cmp::max(existing_status.reblogs_count, status.reblogs_count);
                existing_status.replies_count = std::cmp::max(existing_status.replies_count, status.replies_count);
                existing_status.favourites_count = std::cmp::max(existing_status.favourites_count, status.favourites_count);
            }).or_insert(status);
        }
    }
    for (uri, status) in statuses {
        // Print the URI, reblogs, replies, and favourites counts
        println!("{}: {:?} reblogs, {:?} replies, {:?} favourites", uri, status.reblogs_count, status.replies_count.unwrap_or(0), status.favourites_count);
    }
    Ok(())
}

pub struct Fed;

impl Fed {
    async fn register(server: &str) -> Result<Mastodon> {
        // Check if server already has 'https://' prefix, and remove it if it does
        let server = server.strip_prefix("https://").unwrap_or(server);
        if let Ok(data) = toml::from_file(format!("federation/{}-data.toml", server)) {
            return Ok(Mastodon::from(data));
        }
        let url = format!("https://{}/", server);
        let registration = Registration::new(url)
                                        .client_name("mastodon-async-examples")
                                        .build()
                                        .await?;
        let mastodon = cli::authenticate(registration).await?;
        toml::to_file(&mastodon.data, format!("federation/{}-data.toml", server))?;
        Ok(mastodon)
    }

    pub async fn get_instance(instance_collection: &mut HashMap<String, Mastodon>, server: &str) -> Result<Mastodon> {
        let server = server.strip_prefix("https://").unwrap_or(server);
        if instance_collection.contains_key(server) {
            println!("Instance already registered: {}", server);
        } else {
            println!("Registering new instance: {}", server);
            let instance = Fed::register(server).await?;
            instance_collection.insert(server.to_string(), instance.clone());
        }
        Ok(instance_collection.get(server).unwrap().clone())
    }

    pub async fn me(mastodon: &Mastodon) -> Result<()> {
        let me = mastodon.verify_credentials().await?;
        println!("You are logged in as: {}", me.acct);
        Ok(())
    }

    pub async fn fetch_trending_statuses(base: &str, limit: usize) -> Result<Vec<Status>> {
        println!("Fetching trending statuses from {}", base);
        let base = base.strip_prefix("https://").unwrap_or(base);
        let url = format!("https://{}/api/v1/trends/statuses", base);
        let mut offset = 0;
        let mut trends = Vec::new();
        loop {
            let mut params = HashMap::new();
            params.insert("offset", offset.to_string());
            let response = reqwest::Client::new().get(&url).query(&params).send().await?;
            // Do error handling here for reqwest::Error
            if !response.status().is_success() {
                println!("Error HTTP: {}", response.status());
                break;                
            }
            let json = response.text().await?;
            let json = json.replace(r#""followers_count":-1"#, r#""followers_count":0"#);
            let trending_statuses_raw = serde_json::from_str::<Vec<_>>(&json);
            if trending_statuses_raw.is_err() {
                println!("Error JSON: {}", trending_statuses_raw.err().unwrap());
                break;
            }
            let trending_statuses: Vec<Status> = trending_statuses_raw.unwrap();
            let length_trending_statuses = trending_statuses.len();
            trends.extend(trending_statuses);
            offset += ONE_PAGE;
            if length_trending_statuses < ONE_PAGE || offset >= limit {
                break;
            }
        }
        Ok(trends)
    }
}