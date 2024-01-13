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

use std::collections::{HashMap, HashSet};

use mastodon_async::prelude::*;
use mastodon_async::{helpers::cli, Result};

use colored::Colorize;
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
    let home_server = Fed::get_instance(&mut instance_collection, config_servers_home).await;
    for server in config_servers_authenticated_strings {
        Fed::get_instance(
            &mut instance_collection,
            server.as_str().expect("Server should be a string"),
        )
        .await;
    }
    let mut queued_servers: HashSet<String> = HashSet::new();
    for server in config_servers_unauthenticated_strings {
        queued_servers.insert(server.as_str().expect("Server should be a string").to_string());
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
            tasks.push(Fed::fetch_trending_statuses(&remote.1.data.base, PAGE * 3));
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
                Fed::modify_counts(&mut statuses, status);
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
            tasks.push(Fed::fetch_trending_statuses(server, PAGE * 3));
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
                Fed::modify_counts(&mut statuses, status);
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
            tasks.push(Fed::fetch_status(
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
            tasks.push(Fed::add_context_status(
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

pub struct Fed;

impl Fed {
    async fn register(server: &str) -> Result<Mastodon> {
        if let Ok(data) =
            mastodon_async::helpers::toml::from_file(format!("federation/{server}-data.toml"))
        {
            debug! {"Using cached data for {server}"};
            return Ok(Mastodon::from(data));
        }
        info!("First time registration with {server}");
        let url = format!("https://{server}");
        let registration = Registration::new(url)
            .client_name("mastodon-async-examples")
            .build()
            .await
            .expect("should be a Registration");
        let mastodon = cli::authenticate(registration)
            .await
            .expect("should be an Authentication");
        mastodon_async::helpers::toml::to_file(
            &mastodon.data,
            format!("federation/{server}-data.toml"),
        )
        .expect("should be cached data");
        Ok(mastodon)
    }

    /// Gets an instance from the instance collection, or registers it if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `instance_collection` - The collection of Mastodon instances.
    /// * `server` - The server to get or register.
    ///
    /// # Panics
    ///
    /// This function panics if the server is not registered.
    ///
    /// # Errors
    ///
    /// This function returns an error if there is a problem registering the server.

    pub async fn get_instance(
        instance_collection: &mut HashMap<String, Mastodon>,
        server: &str,
    ) -> Mastodon {
        let server = server.strip_prefix("https://").unwrap_or(server);
        if !instance_collection.contains_key(server) {
            debug!("Registering instance: {server}");
            let instance = Self::register(server).await.expect("should be registered instance");
            instance_collection.insert(server.to_string(), instance.clone());
            return instance;
        }
        instance_collection
            .get(server)
            .expect("should be registered instance")
            .clone()
    }

    /// Gets the current user's account information.
    ///
    /// # Arguments
    ///
    /// * `mastodon` - The Mastodon instance.
    ///
    /// # Panics
    ///
    /// This function panics if there is an error retrieving the current user.
    ///
    /// # Errors
    ///
    /// This function returns an error if there is a problem retrieving the current user.

    pub async fn me(mastodon: &Mastodon) -> Account {
        let me = mastodon.verify_credentials().await.expect("should be current user");
        info!("You are logged in as: {}", me.acct);
        me
    }

    /// Fetches trending statuses from the specified base URL.
    ///
    /// # Arguments
    ///
    /// * `base` - The base URL to fetch the trending statuses from.
    /// * `limit` - The maximum number of trending statuses to fetch.
    ///
    /// # Panics
    ///
    /// This function may panic if there is an error retrieving the trending statuses.
    ///
    /// # Errors
    ///
    /// This function returns a `Result` that may contain an error if there is a problem fetching the trending statuses.

    pub async fn fetch_trending_statuses(base: &str, limit: usize) -> Result<Vec<Status>> {
        info!("Fetching trending statuses from {base}");
        let base = base.strip_prefix("https://").unwrap_or(base);
        let url = format!("https://{base}/api/v1/trends/statuses");
        let mut offset = 0;
        let mut trends = Vec::new();
        loop {
            let mut params = HashMap::new();
            params.insert("offset", offset.to_string());
            let response = reqwest::Client::new().get(&url).query(&params).send().await;
            if response.is_err() {
                error!(
                    "Error HTTP: {}",
                    response.expect_err("Trending statuses error")
                );
                break;
            }
            let response = response.expect("should be trending statuses");
            if !response.status().is_success() {
                error!("Error HTTP: {}", response.status());
                break;
            }
            let json = response.text().await.expect("should be trending statuses");
            let json = json.replace(r#""followers_count":-1"#, r#""followers_count":0"#);
            let trending_statuses_raw = serde_json::from_str::<Vec<_>>(&json);
            if trending_statuses_raw.is_err() {
                error!(
                    "Error JSON: {}",
                    trending_statuses_raw.expect_err("Trending statuses error")
                );
                break;
            }
            let trending_statuses: Vec<Status> = trending_statuses_raw.expect("should be trending statuses");
            let length_trending_statuses = trending_statuses.len();
            trends.extend(trending_statuses);
            offset += PAGE;
            if length_trending_statuses < PAGE || offset >= limit {
                break;
            }
        }
        Ok(trends)
    }

    /// Find the status ID for a given URI.
    ///
    /// # Arguments
    ///
    /// * `uri` - The URI of the status.
    /// * `pool` - The database connection pool.
    /// * `instance_collection` - The collection of Mastodon instances.
    /// * `home` - The home server.
    ///
    /// # Panics
    ///
    /// This function panics if the status is not found in the database.
    ///
    /// # Errors
    ///
    /// This function returns an error if the status is not found by the home server.

    pub async fn find_status_id(uri: &str, pool: &PgPool, home_instance: &Mastodon) -> Result<i64> {
        let select_statement = sqlx::query!(r#"SELECT id FROM statuses WHERE uri = $1"#, uri)
            .fetch_one(pool)
            .await;

        if let Ok(status) = select_statement {
            Ok(status.id)
        } else {
            info!("Status not found in database, searching for it: {uri}");
            let search_result = home_instance.search(uri, true).await;
            if search_result.is_err() {
                let message: String = format!("Status not found by home server: {uri}");
                return Err(mastodon_async::Error::Other(message));
            }
            let search_result = search_result.expect("should be search result");
            if search_result.statuses.is_empty() {
                let message: String = format!("Status not found by home server: {uri}");
                return Err(mastodon_async::Error::Other(message));
            }

            sqlx::query!(r#"SELECT id FROM statuses WHERE uri = $1"#, uri)
                .fetch_one(pool)
                .await
                .map_or_else(
                    |_| {
                        warn!("Status still not found in database, giving up: {uri}");
                        let message: String = format!("Status not found in database: {uri}");
                        Err(mastodon_async::Error::Other(message))
                    },
                    |status| Ok(status.id),
                )
        }
    }

    pub fn modify_counts(statuses: &mut HashMap<String, Status>, status: Status) {
        statuses
            .entry(status.uri.clone())
            .and_modify(|existing_status: &mut Status| {
                debug!(
                    "Duplicate status, Reb: {:?}, Rep: {:?}, Fav: {:?}",
                    existing_status.reblogs_count,
                    existing_status.replies_count.unwrap_or(0),
                    existing_status.favourites_count
                );
                existing_status.reblogs_count =
                    std::cmp::max(existing_status.reblogs_count, status.reblogs_count);
                existing_status.replies_count =
                    std::cmp::max(existing_status.replies_count, status.replies_count);
                existing_status.favourites_count =
                    std::cmp::max(existing_status.favourites_count, status.favourites_count);
            })
            .or_insert(status);
    }

    /// Add a status to the database.
    ///
    /// # Arguments
    ///
    /// * `statuses` - The collection of statuses.
    /// * `status` - The status to add.
    /// * `pool` - The database connection pool.
    /// * `home_server` - The home server.
    ///
    /// # Panics
    ///
    /// This function panics if the status is not found in the database.

    pub async fn add_context_status(
        statuses: &HashMap<String, Status>,
        status: Status,
        pool: &PgPool,
        home_server: &Mastodon,
    ) {
        if statuses.contains_key(&status.uri) {
            debug!("Status already exists, skipping: {}", status.uri);
            return;
        }
        if status.reblogs_count == 0
            && status.replies_count.unwrap_or(0) == 0
            && status.favourites_count == 0
        {
            debug!("Status has no interactions, skipping: {}", status.uri);
            return;
        }
        if let Ok(status_id) = Self::find_status_id(&status.uri, pool, home_server).await {
            debug!("Status found in database: {}", status.uri);
            let select_statement = sqlx::query!(
                r#"SELECT id, reblogs_count, replies_count, favourites_count FROM status_stats WHERE status_id = $1"#,
                status_id
            );
            let select_statement = select_statement.fetch_one(pool).await;

            if select_statement.is_err() {
                info!(
                    "Status not found in status_stats table, inserting it: {}",
                    status.uri
                );
                let offset_date_time = time::OffsetDateTime::now_utc();
                let current_time =
                    time::PrimitiveDateTime::new(offset_date_time.date(), offset_date_time.time());
                let insert_statement = sqlx::query!(
                    r#"INSERT INTO status_stats (status_id, reblogs_count, replies_count, favourites_count, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6)"#,
                    status_id,
                    status.reblogs_count.try_into().unwrap_or_else(|_| {
                        warn!("Failed to convert reblogs_count to i64");
                        0
                    }),
                    status
                        .replies_count
                        .unwrap_or(0)
                        .try_into()
                        .unwrap_or_else(|_| {
                            warn!("Failed to convert replies_count to i64");
                            0
                        }),
                    status.favourites_count.try_into().unwrap_or_else(|_| {
                        warn!("Failed to convert favourites_count to i64");
                        0
                    }),
                    current_time,
                    current_time
                );
                let insert_statement = insert_statement.execute(pool).await;
                if let Err(err) = insert_statement {
                    error!("Error inserting status: {err}");
                    return;
                }
            } else {
                let record = select_statement.expect("should be fetched record from database");
                let old_reblogs_count = record.reblogs_count.try_into().unwrap_or_else(|_| {
                    warn!("Failed to convert reblogs_count to i64");
                    0
                });
                let old_replies_count = record.replies_count.try_into().unwrap_or_else(|_| {
                    warn!("Failed to convert replies_count to i64");
                    0
                });
                let old_favourites_count =
                    record.favourites_count.try_into().unwrap_or_else(|_| {
                        warn!("Failed to convert favourites_count to i64");
                        0
                    });
                if (status.reblogs_count <= old_reblogs_count)
                    && (status.replies_count.unwrap_or(0) <= old_replies_count)
                    && (status.favourites_count <= old_favourites_count)
                {
                    debug!("Status found in status_stats table, but we have smaller counts, skipping: {}", status.uri);
                    return;
                }
                info!(
                    "Status found in status_stats table, updating it: {}",
                    status.uri
                );
                let offset_date_time = time::OffsetDateTime::now_utc();
                let current_time =
                    time::PrimitiveDateTime::new(offset_date_time.date(), offset_date_time.time());
                let update_statement = sqlx::query!(
                    r#"UPDATE status_stats SET reblogs_count = $1, replies_count = $2, favourites_count = $3, updated_at = $4 WHERE status_id = $5"#,
                    status.reblogs_count.try_into().unwrap_or_else(|_| {
                        warn!("Failed to convert reblogs_count to i64");
                        0
                    }),
                    status
                        .replies_count
                        .unwrap_or(0)
                        .try_into()
                        .unwrap_or_else(|_| {
                            warn!("Failed to convert replies_count to i64");
                            0
                        }),
                    status.favourites_count.try_into().unwrap_or_else(|_| {
                        warn!("Failed to convert favourites_count to i64");
                        0
                    }),
                    current_time,
                    status_id
                );
                let update_statement = update_statement.execute(pool).await;
                if let Err(err) = update_statement {
                    error!("Error updating status: {err}");
                    return;
                }
            }
            info!("{} {}", "Status found OK!".green(), status.uri);
        } else {
            debug!("Status not found in database: {}", status.uri);
        }
    }

    /// Fetches a status from the specified URI.
    ///
    /// # Arguments
    ///
    /// * `uri` - The URI of the status.
    /// * `status` - The status to fetch.
    /// * `pool` - The database connection pool.
    /// * `home_server` - The home server.
    /// * `instance_collection` - The collection of Mastodon instances.
    /// * `context_of_statuses` - The collection of statuses to fetch context for.
    ///
    /// # Panics
    ///
    /// This function panics if the status is not found in the database.

    pub async fn fetch_status(
        uri: &str,
        status: &Status,
        pool: &PgPool,
        home_server: &Mastodon,
        instance_collection: &HashMap<String, Mastodon>,
    ) -> HashMap<String, Status> {
        info!("Status: {uri}");
        let status_id = Self::find_status_id(uri, pool, home_server).await;
        let mut context_of_status = HashMap::new();
        if status_id.is_err() {
            warn!("Status not found by home server, skipping: {uri}");
            return context_of_status;
        }
        let status_id = status_id.expect("should be status ID");

        if status.reblogs_count == 0
            && status.replies_count.unwrap_or(0) == 0
            && status.favourites_count == 0
        {
            debug!("Status has no interactions, skipping: {uri}");
            return context_of_status;
        }
        debug!("Status found in database: {uri}");
        let select_statement = sqlx::query!(
            r#"SELECT id, reblogs_count, replies_count, favourites_count FROM status_stats WHERE status_id = $1"#,
            status_id
        );
        let select_statement = select_statement.fetch_one(pool).await;

        let offset_date_time = time::OffsetDateTime::now_utc();
        let current_time =
            time::PrimitiveDateTime::new(offset_date_time.date(), offset_date_time.time());
        if select_statement.is_err() {
            info!("Status not found in status_stats table, inserting it: {uri}");
            let insert_statement = sqlx::query!(
                r#"INSERT INTO status_stats (status_id, reblogs_count, replies_count, favourites_count, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6)"#,
                status_id,
                status.reblogs_count.try_into().unwrap_or_else(|_| {
                    warn!("Failed to convert reblogs_count to i64");
                    0
                }),
                status
                    .replies_count
                    .unwrap_or(0)
                    .try_into()
                    .unwrap_or_else(|_| {
                        warn!("Failed to convert replies_count to i64");
                        0
                    }),
                status.favourites_count.try_into().unwrap_or_else(|_| {
                    warn!("Failed to convert favourites_count to i64");
                    0
                }),
                current_time,
                current_time
            );
            let insert_statement = insert_statement.execute(pool).await;
            if let Err(err) = insert_statement {
                error!("Error inserting status: {err}");
                return context_of_status;
            }
            if status.replies_count.unwrap_or(0) > 0 {
                info!("Fetching context for status: {uri}");
                let original_id =
                    StatusId::new(uri.split('/').last().expect("should be status ID").to_string());
                let original_id_string = uri.split('/').last().expect("should be status ID").to_string();
                // If the original ID isn't alphanumeric, it's probably for a non-Mastodon Fediverse server
                // We can't process these quite yet, so skip them
                if !original_id_string.chars().all(char::is_alphanumeric) {
                    warn!("Original ID is not alphanumeric, skipping: {original_id}");
                    return context_of_status;
                }
                debug!("Original ID: {original_id}");
                let base_server = reqwest::Url::parse(uri)
                    .expect("should be status URI")
                    .host_str()
                    .expect("should be base server string")
                    .to_string();
                let context = if instance_collection.contains_key(&base_server) {
                    let remote = instance_collection
                        .get(&base_server)
                        .expect("should be Mastodon instance");
                    let fetching = remote.get_context(&original_id).await;
                    if fetching.is_err() {
                        error!("Error fetching context");
                        return context_of_status;
                    }
                    fetching.expect("should be Context of Status")
                } else {
                    let url_string = format!(
                        "https://{base_server}/api/v1/statuses/{original_id_string}/context"
                    );
                    let response = reqwest::Client::new().get(&url_string).send().await;
                    if response.is_err() {
                        error!(
                            "{} {}",
                            "Error HTTP:".red(),
                            response.expect_err("Context of Status")
                        );
                        return context_of_status;
                    }
                    let response = response.expect("should be Context of Status");
                    if !response.status().is_success() {
                        error!("{} {}", "Error HTTP:".red(), response.status());
                        return context_of_status;
                    }
                    let json = response.text().await.expect("should be Context of Status");
                    let context = serde_json::from_str::<Context>(&json);
                    if let Err(err) = context {
                        error!("{} {err}", "Error JSON:".red());
                        return context_of_status;
                    }
                    context.expect("should be Context of Status")
                };
                info!(
                    "Fetched context for status: {}, ancestors: {}, descendants: {}",
                    uri,
                    context.ancestors.len(),
                    context.descendants.len()
                );
                for ancestor_status in context.ancestors {
                    context_of_status
                        .entry(ancestor_status.uri.clone())
                        .or_insert(ancestor_status);
                }
                for descendant_status in context.descendants {
                    context_of_status
                        .entry(descendant_status.uri.clone())
                        .or_insert(descendant_status);
                }
            }
        } else {
            let record = select_statement.expect("should be fetched record from database");
            let old_reblogs_count = record.reblogs_count.try_into().unwrap_or_else(|_| {
                warn!("Failed to convert reblogs_count to i64");
                0
            });
            let old_replies_count = record.replies_count.try_into().unwrap_or_else(|_| {
                warn!("Failed to convert replies_count to i64");
                0
            });
            let old_favourites_count = record.favourites_count.try_into().unwrap_or_else(|_| {
                warn!("Failed to convert favourites_count to i64");
                0
            });
            if (status.reblogs_count <= old_reblogs_count)
                && (status.replies_count.unwrap_or(0) <= old_replies_count)
                && (status.favourites_count <= old_favourites_count)
            {
                debug!("Status found in status_stats table, but we don't have larger counts, skipping: {uri}");
                return context_of_status;
            }
            info!("Status found in status_stats table, updating it: {uri}");
            let update_statement = sqlx::query!(
                r#"UPDATE status_stats SET reblogs_count = $1, replies_count = $2, favourites_count = $3, updated_at = $4 WHERE status_id = $5"#,
                status.reblogs_count.try_into().unwrap_or_else(|_| {
                    println!("Failed to convert reblogs_count to i64");
                    0
                }),
                status
                    .replies_count
                    .unwrap_or(0)
                    .try_into()
                    .unwrap_or_else(|_| {
                        println!("Failed to convert replies_count to i64");
                        0
                    }),
                status.favourites_count.try_into().unwrap_or_else(|_| {
                    println!("Failed to convert favourites_count to i64");
                    0
                }),
                current_time,
                status_id
            );
            let update_statement = update_statement.execute(pool).await;
            if let Err(err) = update_statement {
                error!("Error updating status: {err}");
                return context_of_status;
            }
            if status.replies_count.unwrap_or(0) > old_replies_count {
                info!("Fetching context for status: {uri}");
                let original_id =
                    StatusId::new(uri.split('/').last().expect("should be Status ID").to_string());
                let original_id_string = uri.split('/').last().expect("should be Status ID").to_string();
                // If the original ID isn't alphanumeric, it's probably for a non-Mastodon Fediverse server
                // We can't process these quite yet, so skip them
                if !original_id_string.chars().all(char::is_alphanumeric) {
                    warn!("Original ID is not alphanumeric, skipping: {original_id}");
                    return context_of_status;
                }
                debug!("Original ID: {original_id}");
                let base_server = reqwest::Url::parse(uri)
                    .expect("should be Status URI")
                    .host_str()
                    .expect("should be base server string")
                    .to_string();
                let context = if instance_collection.contains_key(&base_server) {
                    let remote = instance_collection
                        .get(&base_server)
                        .expect("should be Mastodon instance");
                    let fetching = remote.get_context(&original_id).await;
                    if fetching.is_err() {
                        error!("Error fetching context");
                        return context_of_status;
                    }
                    fetching.expect("should be Context of Status")
                } else {
                    let url_string = format!(
                        "https://{base_server}/api/v1/statuses/{original_id_string}/context"
                    );
                    let response = reqwest::Client::new().get(&url_string).send().await;
                    if response.is_err() {
                        error!(
                            "{} {}",
                            "Error HTTP:".red(),
                            response.expect_err("Context of Status")
                        );
                        return context_of_status;
                    }
                    let response = response.expect("should be Context of Status");
                    if !response.status().is_success() {
                        error!("{} {}", "Error HTTP:".red(), response.status());
                        return context_of_status;
                    }
                    let json = response.text().await.expect("should be Context of Status");
                    let context = serde_json::from_str::<Context>(&json);
                    if let Err(err) = context {
                        error!("{} {err}", "Error JSON:".red());
                        return context_of_status;
                    }
                    context.expect("should be Context of Status")
                };
                info!(
                    "Fetched context for status: {}, ancestors: {}, descendants: {}",
                    uri,
                    context.ancestors.len(),
                    context.descendants.len()
                );
                for ancestor_status in context.ancestors {
                    context_of_status
                        .entry(ancestor_status.uri.clone())
                        .or_insert(ancestor_status);
                }
                for descendant_status in context.descendants {
                    context_of_status
                        .entry(descendant_status.uri.clone())
                        .or_insert(descendant_status);
                }
            }
        }
        info!("{}", format!("Fetched {uri} OK!").green());
        context_of_status
    }
}
