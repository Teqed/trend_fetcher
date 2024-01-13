// src/main.rs
#![warn(
    clippy::cargo,
    clippy::complexity,
    clippy::correctness,
    clippy::nursery,
    clippy::pedantic,
    clippy::perf,
    clippy::style,
    clippy::suspicious,
    clippy::unwrap_used,
    clippy::question_mark_used
)]
#![allow(clippy::too_many_lines, clippy::multiple_crate_versions)]

mod federation;

use std::collections::{HashMap, HashSet};

use mastodon_async::Result;

use colored::Colorize;
use federation::Federation;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use sqlx::postgres::PgPool;
use toml::Table;
use tracing::{debug, error, info, warn};

const PAGE: usize = 40;
const MAX_FUTURES: usize = 15;

#[tokio::main]
#[tracing::instrument]
async fn main() -> Result<()> {
    debug!("Starting");
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).expect("should be default subscriber");
    let start = time::OffsetDateTime::now_utc();
    let config_string = &std::fs::read_to_string("config.toml").expect("config.toml should exist");
    let config: Table = toml::from_str(config_string).expect("config.toml should be valid TOML");
    let config_servers = config
        .get("servers")
        .expect("'servers' key should be in config.toml")
        .as_table()
        .expect("'servers' value in config.toml should be a table");
    let config_servers_home: &str = config_servers
        .get("home")
        .expect("'home' key should be in 'servers' section of config.toml")
        .as_str()
        .expect("'home' value in config.toml should be a string");
    let config_servers_authenticated_strings = config_servers
        .get("authenticated")
        .expect("'authenticated' key should be in 'servers' section of config.toml")
        .as_array()
        .expect("'authenticated' value in config.toml should be an array");
    let config_servers_unauthenticated_strings = config_servers
        .get("unauthenticated")
        .expect("'unauthenticated' key should be in 'servers' section of config.toml")
        .as_array()
        .expect("'unauthenticated' value in config.toml should be an array");
    let mut instance_collection = HashMap::new();
    let home_server = Federation::get_instance(&mut instance_collection, config_servers_home).await;
    for server in config_servers_authenticated_strings {
        Federation::get_instance(
            &mut instance_collection,
            server.as_str().expect("Server should be a string"),
        )
        .await;
    }
    let mut queued_servers: HashSet<String> = HashSet::new();
    for server in config_servers_unauthenticated_strings {
        queued_servers.insert(
            server
                .as_str()
                .expect("Server should be a string")
                .to_string(),
        );
    }
    let mut statuses = HashMap::new();
    info!("{}", "Fetching trending statuses".green().to_string());
    let mut tasks = FuturesUnordered::new();
    let mut tasks_remaining = instance_collection.len();
    let instance_collection_vec: Vec<_> = instance_collection.iter().collect();
    let mut current_instance_index = 0;
    while tasks_remaining > 0 || !tasks.is_empty() {
        while tasks.len() < MAX_FUTURES && tasks_remaining > 0 {
            tasks_remaining -= 1;
            let remote = &instance_collection_vec[current_instance_index];
            info! {"{}", format!("Fetching trending statuses from {}", remote.1.data.base)};
            tasks.push(Federation::fetch_trending_statuses(
                &remote.1.data.base,
                PAGE * 3,
            ));
            current_instance_index += 1;
        }

        if let Some(fetched_statuses) = tasks.next().await {
            if fetched_statuses.is_err() {
                error! {"{}", format!(
                    "Error fetching trending statuses: {}",
                    fetched_statuses.expect_err("Fetched statuses")
                )};
                continue;
            }
            let fetched_statuses = fetched_statuses.expect("Should be fetched statuses");
            for status in fetched_statuses {
                let base = status
                    .uri
                    .split('/')
                    .nth(2)
                    .expect("Should be FQDN parsed from status URI");
                if !instance_collection.contains_key(base) {
                    // This is a load-bearing comment that prevents the linter from collapsing these statements
                    if queued_servers.insert(base.to_string()) {
                        info! {"{}", format!("Queued server: {base}")};
                    }
                }
                Federation::modify_counts(&mut statuses, status);
            }
        }
    }
    info!("Total statuses: {}", statuses.len());
    info!("Queued servers: {}", queued_servers.len());
    info!(
        "{}",
        "Fetching trending statuses from queued servers".green()
    );
    let mut tasks = FuturesUnordered::new();
    let mut tasks_remaining = queued_servers.len();
    let queued_servers_vec: Vec<_> = queued_servers.iter().collect();
    let mut current_server_index = 0;
    while tasks_remaining > 0 || !tasks.is_empty() {
        while tasks.len() < MAX_FUTURES && tasks_remaining > 0 {
            tasks_remaining -= 1;
            let server = queued_servers_vec[current_server_index];
            info! {"{}", format!("Fetching trending statuses from {}", server)};
            tasks.push(Federation::fetch_trending_statuses(server, PAGE * 3));
            current_server_index += 1;
        }

        if let Some(fetched_statuses) = tasks.next().await {
            if fetched_statuses.is_err() {
                error! {"{}", format!(
                    "Error fetching trending statuses: {}",
                    fetched_statuses.expect_err("Fetched statuses")
                )};
                continue;
            }
            let fetched_statuses = fetched_statuses.expect("Should be fetched statuses");
            for status in fetched_statuses {
                Federation::modify_counts(&mut statuses, status);
            }
        }
    }
    let database_key = config
        .get("database")
        .expect("'database' key should be in config.toml")
        .as_table()
        .expect("'database' value in config.toml should be a table");
    let database_username = database_key
        .get("username")
        .expect("'username' key should be in 'database' section of config.toml")
        .as_str()
        .expect("'username' value in config.toml should be a string");
    let database_password = database_key
        .get("password")
        .expect("'password' key should be in 'database' section of config.toml")
        .as_str()
        .expect("'password' value in config.toml should be a string");
    let database_host = database_key
        .get("host")
        .expect("'host' key should be in 'database' section of config.toml")
        .as_str()
        .expect("'host' value in config.toml should be a string");
    let database_port = database_key
        .get("port")
        .expect("'port' key should be in 'database' section of config.toml")
        .as_integer()
        .expect("'port' value in config.toml should be an integer");
    let database_name = database_key
        .get("name")
        .expect("'name' key should be in 'database' section of config.toml")
        .as_str()
        .expect("'name' value in config.toml should be a string");
    let database_url = format!("postgres://{database_username}:{database_password}@{database_host}:{database_port}/{database_name}");
    let pool = PgPool::connect(&database_url)
        .await
        .expect("should be a connection to Postgresql database");
    info!("{}", "Inserting or updating statuses".green());
    let mut context_of_statuses = HashMap::new();
    let mut tasks = FuturesUnordered::new();
    let mut tasks_remaining = statuses.len();
    let statuses_vec: Vec<_> = statuses.iter().collect();
    let mut current_status_index = 0;
    while tasks_remaining > 0 || !tasks.is_empty() {
        while tasks.len() < MAX_FUTURES && tasks_remaining > 0 {
            tasks_remaining -= 1;
            let (uri, status) = statuses_vec[current_status_index];
            tasks.push(Federation::fetch_status(
                uri,
                status,
                &pool,
                &home_server,
                &instance_collection,
            ));
            current_status_index += 1;
        }

        if let Some(context_of_status) = tasks.next().await {
            for (uri, status) in context_of_status {
                context_of_statuses.entry(uri).or_insert(status);
            }
        }
    }

    info! {"{}", format!("Fetching context statuses from {}", home_server.data.base)};
    let mut tasks = FuturesUnordered::new();
    let context_of_statuses_length = context_of_statuses.len();
    let mut tasks_remaining = context_of_statuses_length;
    let context_of_statuses_vec: Vec<_> = context_of_statuses.iter().collect();
    let mut current_context_of_status_index = 0;
    while tasks_remaining > 0 || !tasks.is_empty() {
        while tasks.len() < MAX_FUTURES && tasks_remaining > 0 {
            tasks_remaining -= 1;
            let (_, status) = context_of_statuses_vec[current_context_of_status_index];
            tasks.push(Federation::add_context_status(
                &statuses,
                status.clone(),
                &pool,
                &home_server,
            ));
            current_context_of_status_index += 1;
        }

        if (tasks.next().await).is_some() {
            continue;
        }
    }

    info!("{}", "All OK!".green());
    info!("We saw {} statuses", statuses.len());
    info!("We saw {context_of_statuses_length} context statuses");
    let end = time::OffsetDateTime::now_utc();
    let duration = end - start;
    info!("Duration: {duration}");
    Ok(())
}
