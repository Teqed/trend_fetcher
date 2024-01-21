use std::collections::HashMap;
use async_recursion::async_recursion;

use mastodon_async::entities::search_result;
use mastodon_async::prelude::*;
use mastodon_async::{helpers::cli, Result};

use crate::PAGE;
use colored::Colorize;
use sqlx::postgres::PgPool;
use tracing::{debug, error, info, warn};

/// Struct for interacting with Fediverse APIs.
pub struct Federation;

/// Implementation of Federation.
impl Federation {
    /// Registers a server with the Mastodon instance.
    pub(crate) async fn register(server: &str) -> Result<Mastodon> {
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
    pub async fn get_instance(
        instance_collection: &mut HashMap<String, Mastodon>,
        server: &str,
    ) -> Mastodon {
        let server = server.strip_prefix("https://").unwrap_or(server);
        if !instance_collection.contains_key(server) {
            debug!("Registering instance: {server}");
            let instance = Self::register(server)
                .await
                .expect("should be registered instance");
            instance_collection.insert(server.to_string(), instance.clone());
            return instance;
        }
        instance_collection
            .get(server)
            .expect("should be registered instance")
            .clone()
    }

    /// Fetches trending hashtags from the specified instance.
    pub async fn fetch_trending_statuses(base: &str) -> Result<Vec<Status>> {
        info!("Fetching trending statuses from {base}");
        let base = base.strip_prefix("https://").unwrap_or(base);
        let url = format!("https://{base}/api/v1/trends/statuses");
        let mut offset = 0;
        let limit = 40; // Mastodon API limit; default 20, max 40
        let mut trends = Vec::new();
        loop {
            let mut params = HashMap::new();
            params.insert("offset", offset.to_string());
            params.insert("limit", limit.to_string());
            let response = reqwest::Client::new().get(&url).query(&params).send().await;
            if response.is_err() {
                error!(
                    "Error HTTP: {}",
                    response.expect_err("Trending statuses error")
                );
                warn!("Encountered on {}", url.red());
                break;
            }
            let response = response.expect("should be trending statuses");
            if !response.status().is_success() {
                error!("Error HTTP: {}", response.status());
                break;
            }
            let json = response.text().await.expect("should be trending statuses");
            info!("Reading Trending JSON from {}", url);
            let trending_statuses_raw: std::prelude::v1::Result<Vec<Status>, serde_json::Error> =
                serde_json::from_str(&json);
            if trending_statuses_raw.is_err() {
                warn!("Parsing Issue on Trending JSON");
                let error = trending_statuses_raw.expect_err("Trending statuses error");
                error!("Error JSON: {}", error);
                json_window(&error, &json);
                break;
            }
            let trending_statuses: Vec<_> =
                trending_statuses_raw.expect("should be trending statuses");
            let length_trending_statuses = trending_statuses.len();
            trends.extend(trending_statuses);
            offset += PAGE;
            if length_trending_statuses < PAGE {
                break;
            }
        }
        Ok(trends)
    }

    /// Find the status ID for a given URI from the home instance's database.
    #[async_recursion]
    pub async fn find_status_id(uri: &str, pool: &PgPool, home_instance: &Mastodon) -> Result<i64> {
        let select_statement = sqlx::query!(r#"SELECT id FROM statuses WHERE uri = $1"#, uri)
            .fetch_one(pool)
            .await;

        if let Ok(status) = select_statement {
            Ok(status.id)
        } else {
            debug!("Status not found in database, searching for it: {uri}");
            let search_result = home_instance.search(uri, true).await;
            if search_result.is_err() {
                warn!("Error searching for status: {uri}");
                return Err(search_result.expect_err("should be search result"));
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

    /// Modify the counts of a status.
    pub fn modify_counts(statuses: &mut HashMap<String, Status>, status: Status) {
        statuses
            .entry(status.uri.to_string())
            .and_modify(|existing_status: &mut Status| {
                debug!(
                    "Duplicate status, Reb: {:?}, Rep: {:?}, Fav: {:?}",
                    existing_status.reblogs_count,
                    existing_status.replies_count,
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

    /// Fetches a status from the specified URI.
    pub async fn fetch_status(
        status: &Status,
        pool: &PgPool,
        home_server: &Mastodon,
        instance_collection: &HashMap<String, Mastodon>,
    ) -> Result<Option<HashMap<String, Status>>> {
        info!("Status: {}", &status.uri);
        let mut additional_context_statuses = HashMap::new();
        let status_id = Self::find_status_id(status.uri.as_ref(), pool, home_server).await;
        if status_id.is_err() {
            warn!("Status not found by home server, skipping: {}", &status.uri);
            return Ok(None);
        }
        let status_id = status_id.expect("should be status ID");
        if status.reblogs_count == 0 && status.replies_count <= 0 && status.favourites_count == 0 {
            debug!("Status has no interactions, skipping: {}", &status.uri);
            return Ok(None);
        }
        debug!("Status found in database: {}", &status.uri);
        let select_statement = sqlx::query!(
            r#"SELECT id, reblogs_count, replies_count, favourites_count FROM status_stats WHERE status_id = $1"#,
            status_id
        );
        let select_statement = select_statement.fetch_one(pool).await;
        match select_statement {
            Ok(record) => {
                let reblogs_count = record.reblogs_count;
                let replies_count = record.replies_count;
                let favourites_count = record.favourites_count;
                let descendants = update_status(
                    reblogs_count,
                    replies_count,
                    favourites_count,
                    status,
                    status_id,
                    pool,
                    instance_collection,
                )
                .await;
                if let Some(descendant_status) = descendants {
                    for child in descendant_status {
                        additional_context_statuses
                            .entry(child.uri.to_string().clone())
                            .or_insert(child);
                    }
                }
            }
            Err(err) => {
                debug!("Error fetching status_stats: {err}");
                debug!(
                    "Status not found in status_stats table, inserting it: {}",
                    &status.uri
                );
                let descendants = insert_status(status_id, status, pool, instance_collection).await;
                if let Some(descendant_status) = descendants {
                    for child in descendant_status {
                        additional_context_statuses
                            .entry(child.uri.to_string().clone())
                            .or_insert(child);
                    }
                }
            }
        }
        debug!("{}", format!("Fetched {} OK!", &status.uri).green());
        Ok(Some(additional_context_statuses))
    }
}

async fn update_status(
    reblogs_count: i64,
    replies_count: i64,
    favourites_count: i64,
    status: &Status,
    status_id: i64,
    pool: &sqlx::Pool<sqlx::Postgres>,
    instance_collection: &HashMap<String, Mastodon>,
) -> Option<Vec<Status>> {
    let offset_date_time = time::OffsetDateTime::now_utc();
    let current_time =
        time::PrimitiveDateTime::new(offset_date_time.date(), offset_date_time.time());
    if (status.reblogs_count <= reblogs_count)
        && (status.replies_count <= replies_count)
        && (status.favourites_count <= favourites_count)
    {
        debug!(
            "Status found in status_stats table, but we don't have larger counts, skipping: {}",
            &status.uri
        );
        return None;
    }
    debug!(
        "Status found in status_stats table, updating it: {}",
        &status.uri
    );
    let update_statement = sqlx::query!(
        r#"UPDATE status_stats SET reblogs_count = $1, favourites_count = $2, updated_at = $3 WHERE status_id = $4"#,
        std::cmp::max(
            status.reblogs_count.try_into().unwrap_or_else(|_| {
                warn!("Failed to convert reblogs_count to i64");
                0
            }),
            reblogs_count
        ),
        std::cmp::max(
            status.favourites_count.try_into().unwrap_or_else(|_| {
                warn!("Failed to convert favourites_count to i64");
                0
            }),
            favourites_count
        ),
        current_time,
        status_id
    );
    let update_statement = update_statement.execute(pool).await;
    if let Err(err) = update_statement {
        error!("Error updating status: {err}");
        return None;
    }
    if status.replies_count > replies_count {
        let descendants = get_status_descendants(status, instance_collection).await;
        if descendants.is_none() {
            warn!("Error fetching descendants");
            return None;
        }
        let descendants = descendants.expect("should be descendants");
        return Some(descendants);
    }
    None
}

async fn insert_status(
    status_id: i64,
    status: &Status,
    pool: &sqlx::Pool<sqlx::Postgres>,
    instance_collection: &HashMap<String, Mastodon>,
) -> Option<Vec<Status>> {
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
        0,
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
        return None;
    }
    if status.replies_count > 0 {
        let descendants = get_status_descendants(status, instance_collection).await;
        if descendants.is_none() {
            warn!("Error fetching descendants");
            return None;
        }
        let descendants = descendants.expect("should be descendants");
        return Some(descendants);
    }
    None
}

async fn get_status_descendants(
    status: &Status,
    instance_collection: &HashMap<String, Mastodon>,
) -> Option<Vec<Status>> {
    debug!("Fetching context for status: {}", &status.uri);
    let original_id = StatusId::new(
        status
            .uri
            .to_string()
            .split('/')
            .last()
            .expect("should be Status ID")
            .to_string(),
    );
    let original_id_string = &status
        .uri
        .to_string()
        .split('/')
        .last()
        .expect("should be Status ID")
        .to_string();
    if !original_id_string.chars().all(char::is_alphanumeric) {
        warn!("Original ID is not alphanumeric, skipping: {original_id}");
        return None;
    }
    debug!("Original ID: {original_id}");
    if status.in_reply_to_id.is_some() {
        debug!("Status is a reply, skipping: {}", &status.uri);
        return None;
    }
    let base_server = reqwest::Url::parse(status.uri.as_ref())
        .expect("should be Status URI")
        .host_str()
        .expect("should be base server string")
        .to_string();
    let Some(context) = get_status_context(instance_collection, base_server, original_id).await
    else {
        warn!("Error fetching context");
        return None;
    };
    info!(
        "Fetched context for status: {}, ancestors: {}, descendants: {}",
        &status.uri,
        context.ancestors.len(),
        context.descendants.len()
    );
    Some(context.descendants)
}

async fn get_status_context(
    instance_collection: &HashMap<String, Mastodon>,
    base_server: String,
    original_id: StatusId,
) -> Option<Context> {
    let original_id_string = &original_id.to_string();
    let context = if instance_collection.contains_key(&base_server) {
        let remote = instance_collection
            .get(&base_server)
            .expect("should be Mastodon instance");
        let fetching = remote.get_context(&original_id).await;
        if fetching.is_err() {
            error!("Error fetching context");
            return None;
        }
        fetching.expect("should be Context of Status")
    } else {
        let url_string =
            format!("https://{base_server}/api/v1/statuses/{original_id_string}/context");
        let response = reqwest::Client::new().get(&url_string).send().await;
        if response.is_err() {
            error!(
                "{} {}",
                "Error HTTP:".red(),
                response.expect_err("Context of Status")
            );
            return None;
        }
        let response = response.expect("should be Context of Status");
        if !response.status().is_success() {
            error!("{} {}", "Error HTTP:".red(), response.status());
            return None;
        }
        let json = response.text().await.expect("should be Context of Status");
        let context = serde_json::from_str::<Context>(&json);
        if let Err(err) = context {
            error!("{} {err}", "Error JSON:".red());
            json_window(&err, &json);
            return None;
        }
        context.expect("should be Context of Status")
    };
    Some(context)
}

fn json_window(err: &serde_json::Error, json: &String) {
    let column = err
        .to_string()
        .split("column ")
        .last()
        .expect("should be column")
        .parse::<usize>()
        .expect("should be usize");
    let json = json.as_bytes();
    let mut start = column;
    let mut end = column;
    for _ in 0..200 {
        if start == 0 {
            break;
        }
        start -= 1;
    }
    for _ in 0..200 {
        if end == json.len() {
            break;
        }
        end += 1;
    }
    let json = String::from_utf8_lossy(&json[start..end]);
    error!("Error JSON preview window: {}", json);
    // wait until 'enter' is pressed
    // let mut input = String::new();
    // std::io::stdin()
    //     .read_line(&mut input)
    //     .expect("should be able to read line");
}
