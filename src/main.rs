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
    // restrictions
    // clippy::question_mark_used,
    clippy::panic_in_result_fn,
    clippy::print_stderr,
    clippy::print_stdout,
    clippy::cognitive_complexity,
    clippy::dbg_macro,
    clippy::debug_assert_with_mut_call,
    clippy::doc_link_with_quotes,
    clippy::doc_markdown,
    clippy::empty_line_after_outer_attr,
    clippy::empty_structs_with_brackets,
    clippy::float_cmp,
    clippy::float_cmp_const,
    clippy::float_equality_without_abs,
    keyword_idents,
    clippy::missing_const_for_fn,
    missing_copy_implementations,
    missing_debug_implementations,
    // clippy::missing_docs_in_private_items,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::mod_module_files,
    non_ascii_idents,
    noop_method_call,
    clippy::option_if_let_else,
    clippy::semicolon_if_nothing_returned,
    clippy::unseparated_literal_suffix,
    clippy::shadow_unrelated,
    clippy::similar_names,
    clippy::suspicious_operation_groupings,
    unused_crate_dependencies,
    unused_extern_crates,
    unused_import_braces,
    clippy::unused_self,
    clippy::use_debug,
    clippy::used_underscore_binding,
    clippy::useless_let_if_seq,
    clippy::wildcard_dependencies,
    clippy::wildcard_imports,
    clippy::enum_glob_use,
    clippy::exit,
    clippy::map_err_ignore,
    clippy::mem_forget,
    clippy::rc_mutex,
    clippy::rest_pat_in_fully_bound_structs,
    clippy::string_add,
    clippy::string_to_string,
    clippy::todo,
    clippy::unimplemented,
    clippy::verbose_file_reads,
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    trivial_casts,
    trivial_numeric_casts,
    unused_qualifications,
    variant_size_differences,
    clippy::mem_forget,
)]
#![allow(clippy::multiple_crate_versions, clippy::too_many_lines)]
#![forbid(unsafe_code)]

use color_eyre::eyre::Result;
use colored::Colorize;
use federation::Federation;
use futures::stream::{self, StreamExt};
use reqwest_middleware::ClientWithMiddleware;
use serde::Deserialize;
use sqlx::postgres::PgPool;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs;
use tokio::task;
use tracing::{debug, error, info, warn};

#[derive(Deserialize)]
/// Configuration struct for the application.
struct Config {
    /// Database configuration.
    pub database: DatabaseConfig,
    /// Rocket configuration.
    pub rocket: RocketConfig,
    /// Servers configuration.
    pub servers: ServersConfig,
}

#[derive(Deserialize)]
/// Rocket configuration.
struct RocketConfig {
    /// Rocket hostname.
    pub hostname: String,
}

#[derive(Deserialize)]
/// Servers configuration.
struct ServersConfig {
    /// Home server.
    pub home: String,
    /// Authenticated servers.
    pub authenticated: Vec<String>,
    /// Unauthenticated servers.
    pub unauthenticated: Vec<String>,
}

#[derive(Deserialize)]
/// Database configuration.
struct DatabaseConfig {
    /// Database username.
    pub username: String,
    /// Database password.
    pub password: String,
    /// Database host.
    pub host: String,
    /// Database port.
    pub port: i64,
    /// Database name.
    pub name: String,
}
/// The federation module provides functions for interacting with the Mastodon API.
mod federation;

/// The number of statuses to fetch per page, a maximum defined by the Mastodon API.
const PAGE: usize = 40;
/// The maximum number of futures to run concurrently.
const MAX_FUTURES: usize = 15;

#[derive(Clone)]
enum Instance {
    InstanceWithToken {
        client: ClientWithMiddleware,
        token: String,
    },
    InstanceWithoutToken {
        client: ClientWithMiddleware,
    },
}

impl Token for Instance {
    fn token(&self) -> Option<String> {
        match self {
            Self::InstanceWithToken { token, .. } => Some(token.to_string()),
            Self::InstanceWithoutToken { .. } => None,
        }
    }
}

impl Client for Instance {
    fn client(&self) -> ClientWithMiddleware {
        match self {
            Self::InstanceWithoutToken { client } | Self::InstanceWithToken { client, .. } => {
                client.clone()
            }
        }
    }
}

trait Token {
    fn token(&self) -> Option<String>;
}

trait Client {
    fn client(&self) -> ClientWithMiddleware;
}

#[tokio::main]
#[tracing::instrument]
/// The main function.
async fn main() -> Result<(), Box<dyn Error>> {
    let start = time::OffsetDateTime::now_utc();
    debug!("Starting at {start}");
    color_eyre::install()?;
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::new())
        .expect("should be default subscriber");
    let mut instance_collection = HashMap::new();
    let config: Config = match fs::read_to_string("config.toml") {
        Ok(config) => match toml::from_str(&config) {
            Ok(config) => config,
            Err(e) => {
                error!("{}", "Error loading configuration. Please ensure that the configuration file is valid TOML".to_string().red());
                error!("{}", "See the example configuration file `config.toml.example` for more information on required fields".to_string().red());
                error!("{}", format!("{}", e.to_string().red()));
                return Err(e.into());
            }
        },
        Err(e) => {
            error!(
                "{}",
                "Error loading configuration. Please ensure that the configuration file exists"
                    .to_string()
                    .red()
            );
            error!(
                "{}",
                "See the example configuration file `config.toml.example`"
                    .to_string()
                    .red()
            );
            error!("{}", format!("{}", e.to_string().red()));
            return Err(e.into());
        }
    };
    let rocket_hostname = config.rocket.hostname.clone();
    let mut queued_servers: HashSet<String> = HashSet::new();
    let home_server_string = config.servers.home.clone();
    let home_server: Instance =
        Federation::get_instance(&mut instance_collection, &home_server_string, true).await;
    for server in config.servers.authenticated {
        Federation::get_instance(&mut instance_collection, &server, true).await;
        queued_servers.insert(server);
    }
    config.servers.unauthenticated.iter().for_each(|server| {
        queued_servers.insert(server.to_string());
    });
    info!("{}", "Fetching trending statuses".green().to_string());
    let mut queued_statuses = HashMap::new();
    let mut fetched_servers = HashSet::new();
    while !queued_servers.is_empty() {
        info!("Queued servers: {}", queued_servers.len());
        info!(
            "{}",
            "Fetching trending statuses from queued servers".green()
        );
        let mut queued_instances = HashMap::new();
        for server in queued_servers.iter().cloned() {
            let instance = Federation::get_instance(&mut instance_collection, &server, false).await;
            queued_instances.insert(server, instance);
        }
        let fetched_trending_statuses_vec = stream::iter(queued_instances.into_iter())
            .map(|instance| async move {
                Federation::fetch_trending_statuses(&instance.0, &instance.1).await
            })
            .buffer_unordered(MAX_FUTURES * 4)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("Error fetching trending statuses from queued servers");
        fetched_servers.extend(queued_servers.iter().cloned());
        queued_servers.clear();
        let aux_trending_statuses_hashmap = {
            let mut fetched_trending_statuses = HashMap::new();
            for status in fetched_trending_statuses_vec.into_iter().flatten() {
                let base = status.uri.to_string();
                let base = base
                    .split('/')
                    .nth(2)
                    .expect("Error getting base from status URI");
                if !fetched_servers.contains(base) {
                    // This is a load-bearing comment that prevents the linter from collapsing these statements
                    if queued_servers.insert(base.to_string()) {
                        debug! {"{}", format!("Queued server: {base}")};
                    }
                }
                Federation::modify_counts(&mut fetched_trending_statuses, status);
            }
            fetched_trending_statuses
        };
        for (_, value) in aux_trending_statuses_hashmap {
            Federation::modify_counts(&mut queued_statuses, value);
        }
    }
    info!("Total statuses: {}", queued_statuses.len());

    let pool = PgPool::connect(&format!(
        "postgres://{database_username}:{database_password}@{database_host}:{database_port}/{database_name}",
        database_username = config.database.username,
        database_password = config.database.password,
        database_host = config.database.host,
        database_port = config.database.port,
        database_name = config.database.name
    ))
        .await
        .expect("should be a connection to Postgresql database");
    info!("{}", "Inserting or updating statuses".green());
    let mut fetched_statuses = HashMap::new();
    let length_of_initial_statuses = queued_statuses.len();
    while !queued_statuses.is_empty() {
        info!("Queued statuses: {}", queued_statuses.len());
        let cloned_queued_statuses = queued_statuses.clone();
        info!("Starting Rocket");
        task::spawn(async move {
            Federation::start_rocket(cloned_queued_statuses).await;
        });
        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
        info!("Rocket started");
        let mut queued_instances = HashMap::new();
        for (uri, status) in queued_statuses.clone() {
            let instance =
                Federation::get_instance(&mut instance_collection, status.uri.as_ref(), false)
                    .await;
            queued_instances.insert(uri, (status, instance));
        }
        let some_context = stream::iter(queued_instances.clone().into_iter())
            .map(|(_, (status, _))| {
                let home_server_string = home_server_string.clone();
                let home_instance = home_server.clone();
                let pool = pool.clone();
                let instance_collection = instance_collection.clone();
                let rocket_hostname = rocket_hostname.clone();
                async move {
                    Federation::fetch_status(
                        &status,
                        &pool,
                        &home_server_string,
                        &home_instance,
                        &rocket_hostname,
                        &instance_collection,
                    )
                    .await
                }
            })
            .buffer_unordered(MAX_FUTURES)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("Error fetching context statuses from queued statuses");
        info!("Shutting down Rocket");
        let request_shutdown = reqwest::Client::new()
            .post("https://{rocket_hostname}/shutdown")
            .send()
            .await;

        for (_, status) in queued_statuses.clone() {
            Federation::modify_counts(&mut fetched_statuses, status);
        }
        queued_statuses.clear();
        for status_map in some_context {
            if status_map.is_none() {
                continue;
            }
            for (_, status) in status_map.expect("should be a status map") {
                if fetched_statuses.contains_key(&status.uri.to_string()) {
                    Federation::modify_counts(&mut fetched_statuses, status);
                    continue;
                }
                Federation::modify_counts(&mut queued_statuses, status);
            }
        }
        if let Err(e) = request_shutdown {
            error!("{}", "Error shutting down Rocket".red());
            error!("{}", format!("{}", e.to_string().red()));
            return Err(e.into());
        }
        info!("Rocket shut down");
    }
    info!("{}", "All OK!".green());
    info!("We saw {} trending statuses", length_of_initial_statuses);
    info!(
        "We saw {} context statuses",
        fetched_statuses.len() - length_of_initial_statuses
    );
    let end = time::OffsetDateTime::now_utc();
    let duration = end - start;
    info!("Duration: {duration}");
    Ok(())
}
