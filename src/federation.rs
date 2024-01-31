use async_recursion::async_recursion;
use std::collections::HashMap;
use std::time::Duration;

use mastodon_async::prelude::*;
use mastodon_async::{helpers::cli, Result};
use reqwest::Url;

use crate::{Client, Token, PAGE};
use colored::Colorize;
use sqlx::postgres::PgPool;
use tracing::{debug, error, info, warn};

use reqwest_middleware::ClientBuilder;
use reqwest_retry_after::RetryAfterMiddleware;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use rocket::{self, State};

#[derive(Debug, Serialize, Deserialize)]
struct ActivityPubNote {
    #[serde(rename = "@context")]
    context: Vec<Value>,
    id: Url,
    #[serde(rename = "type")]
    type_: String,
    summary: Option<Value>,
    #[serde(rename = "inReplyTo")]
    in_reply_to: Option<Value>,
    // published: OffsetDateTime,
    published: String,
    url: Url,
    uri: Url,
    #[serde(rename = "attributedTo")]
    attributed_to: Option<String>,
    to: Vec<String>,
    cc: Vec<String>,
    sensitive: bool,
    #[serde(rename = "atomUri")]
    atom_uri: Option<Url>,
    #[serde(rename = "inReplyToAtomUri")]
    in_reply_to_atom_uri: Option<Url>,
    conversation: String,
    content: String,
    #[serde(rename = "contentMap")]
    content_map: serde_json::Map<String, Value>,
    attachment: Vec<APAttachment>,
    tag: Vec<APTag>,
    replies: APReplies,
}

#[derive(Debug, Serialize, Deserialize)]
struct APAttachment {
    #[serde(rename = "type")]
    type_: String,
    #[serde(rename = "mediaType")]
    media_type: String,
    url: String,
    name: Option<Value>,
    blurhash: String,
    #[serde(rename = "focalPoint")]
    focal_point: Vec<i64>,
    width: Option<i64>,
    height: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct APTag {
    #[serde(rename = "type")]
    type_: String,
    href: String,
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct APReplies {
    id: String,
    #[serde(rename = "type")]
    type_: String,
    first: APFirst,
}

#[derive(Debug, Serialize, Deserialize)]
struct APFirst {
    #[serde(rename = "type")]
    type_: String,
    next: String,
    #[serde(rename = "partOf")]
    part_of: Option<String>,
    items: Vec<Value>,
}
struct AppState(Vec<ActivityPubNote>);

use crate::Instance;
use rocket::request::Request;
use rocket::response::{self, Responder, Response};
use rocket::serde::json::Json;

struct ActivityJsonResponder {
    inner: Json<ActivityPubNote>,
}

impl<'r> Responder<'r, 'static> for ActivityJsonResponder {
    fn respond_to(self, req: &'r Request<'_>) -> response::Result<'static> {
        Response::build_from(self.inner.respond_to(req)?)
            .raw_header("Content-Type", "application/activity+json")
            .ok()
    }
}

use rocket::Shutdown;

#[rocket::post("/shutdown")]
fn shutdown(shutdown: Shutdown) -> &'static str {
    shutdown.notify();
    "Shutting down..."
}
#[rocket::get("/users/<user>/statuses/<status_id>")]
fn search(
    user: &str,
    status_id: &str,
    route_json: &State<AppState>,
) -> Option<ActivityJsonResponder> {
    let status = route_json
        .0
        .iter()
        .find(|status| status.id.path() == format!("/users/{user}/statuses/{status_id}"))?;
    let status = serde_json::to_value(status).expect("should be valid json");
    let status: ActivityPubNote = serde_json::from_value(status).expect("should be valid status");
    Some(ActivityJsonResponder {
        inner: Json(status),
    })
}

fn convert_status_to_activitypub(status: &Status) -> ActivityPubNote {
    let media_attachments: Vec<APAttachment> = status
        .media_attachments
        .iter()
        .map(|media| {
            let meta = media.meta.as_ref().and_then(|m| m.original.as_ref());
            let media_clone = media.url.clone().expect("should be url").to_string();
            let extension = media_clone.split('.').last().expect("should be extension");
            let type_from_media = match media.media_type {
                MediaType::Image => match extension {
                    "png" => "image/png".to_string(),
                    "jpg" | "jpeg" => "image/jpeg".to_string(),
                    "gif" => "image/gif".to_string(),
                    _ => "image".to_string(),
                },
                MediaType::Video | MediaType::Gifv => match extension {
                    "mp4" => "video/mp4".to_string(),
                    "webm" => "video/webm".to_string(),
                    _ => "video".to_string(),
                },
                MediaType::Audio => {
                    let media_clone_secondary =
                        media.url.clone().expect("should be url").to_string();
                    let extension_secondary = media_clone_secondary
                        .split('.')
                        .last()
                        .expect("should be extension");
                    match extension_secondary {
                        "mp3" => "audio/mp3".to_string(),
                        "ogg" => "audio/ogg".to_string(),
                        _ => "audio".to_string(),
                    }
                }
                MediaType::Unknown => "unknown".to_string(),
            };
            APAttachment {
                type_: "Document".to_string(),
                media_type: type_from_media,
                url: media
                    .remote_url
                    .clone()
                    .unwrap_or_else(|| media.url.clone().expect("should be url"))
                    .to_string(),
                name: media.description.clone().map(Value::String),
                blurhash: media.blurhash.clone().unwrap_or_default(),
                focal_point: vec![0, 0],
                width: meta.and_then(|m| m.width),
                height: meta.and_then(|m| m.height),
            }
        })
        .collect();

    let context = vec![
        Value::String("https://www.w3.org/ns/activitystreams".to_string()),
        serde_json::json!({
            "ostatus": "http://ostatus.org#",
            "atomUri": "ostatus:atomUri",
            "inReplyToAtomUri": "ostatus:inReplyToAtomUri",
            "conversation": "ostatus:conversation",
            "sensitive": "as:sensitive",
            "toot": "http://joinmastodon.org/ns#",
            "votersCount": "toot:votersCount",
            "blurhash": "toot:blurhash",
            "focalPoint": {
                "@container": "@list",
                "@id": "toot:focalPoint"
            },
            "Hashtag": "as:Hashtag"
        }),
    ];

    let language_code = &status.language;
    let content_map = serde_json::Map::from_iter(vec![(
        language_code.clone().unwrap_or_default(),
        serde_json::Value::String(status.content.clone()),
    )]);

    ActivityPubNote {
        context,
        id: status.uri.clone(),
        type_: "Note".to_string(),
        summary: None,
        in_reply_to: None,
        published: status.created_at.to_string(),
        url: status.url.clone().expect("should be url"),
        uri: status.uri.clone(),
        attributed_to: Some(
            status
                .account
                .uri
                .clone()
                .unwrap_or_else(|| status.account.url.clone())
                .to_string(),
        ),
        to: vec!["https://www.w3.org/ns/activitystreams#Public".to_string()],
        cc: vec![format!(
            "{}/followers",
            status
                .account
                .uri
                .clone()
                .unwrap_or_else(|| status.account.url.clone())
        )],
        sensitive: status.sensitive,
        atom_uri: Some(status.uri.clone()),
        in_reply_to_atom_uri: None,
        conversation: String::new(),
        content: status.content.clone(),
        content_map,
        attachment: media_attachments,
        tag: status
            .tags
            .iter()
            .map(|tag| APTag {
                type_: "Hashtag".to_string(),
                href: tag.url.clone(),
                name: format!("#{}", tag.name),
            })
            .collect(),
        replies: APReplies {
            id: format!("{}/replies", status.uri.clone()),
            type_: "Collection".to_string(),
            first: APFirst {
                type_: "CollectionPage".to_string(),
                next: format!(
                    "{}/replies?only_other_accounts=true&page=true",
                    status.uri.clone()
                ),
                part_of: Some(format!("{}/replies", status.uri.clone())),
                items: Vec::new(),
            },
        },
    }
}

/// Struct for interacting with Fediverse APIs.
pub struct Federation;

/// Implementation of Federation.
impl Federation {
    /// Registers a server with the Mastodon instance.
    pub async fn register(server: &str) -> Result<Mastodon> {
        if let Ok(data) =
            mastodon_async::helpers::toml::from_file(format!("federation/{server}-data.toml"))
        {
            debug! {"Using cached data for {server}"};
            return Ok(Mastodon::from(data));
        }
        info!("First time registration with {server}");
        let url = format!("https://{server}");
        let registration = Registration::new(url)
            .client_name("Mastodon API Client")
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

    pub async fn get_instance(
        client_collection: &mut HashMap<String, Instance>,
        server: &str,
        should_register: bool,
    ) -> Instance {
        let server = server.strip_prefix("https://").unwrap_or(server);
        if !client_collection.contains_key(server) {
            debug!("Creating new client for instance: {server}");
            if should_register {
                let client = ClientBuilder::new(reqwest::Client::new())
                    .with(RetryAfterMiddleware::new())
                    .build();
                let mastodon_instance = Self::register(server).await.expect("should be Mastodon");
                let token = mastodon_instance.data.token.to_string();
                let instance = Instance::InstanceWithToken { client, token };
                client_collection.insert(server.to_string(), instance.clone());
                return instance;
            }
            let client = ClientBuilder::new(reqwest::Client::new())
                .with(RetryAfterMiddleware::new())
                .build();
            let instance = Instance::InstanceWithoutToken { client };
            client_collection.insert(server.to_string(), instance.clone());
            return instance;
        }
        let instance = client_collection
            .get(server)
            .expect("should be registered instance");
        instance.clone()
    }

    /// Fetches trending hashtags from the specified instance.
    pub async fn fetch_trending_statuses(base: &str, instance: &Instance) -> Result<Vec<Status>> {
        debug!("Fetching trending statuses from {base}");
        let fetch_timer_start = std::time::Instant::now();
        let client = instance.client();
        let base = base
            .strip_prefix("https://")
            .unwrap_or(base)
            .strip_suffix('/')
            .unwrap_or(base);
        let endpoint = "/api/v1/trends/statuses";
        let url = format!("https://{base}{endpoint}");
        let mut offset = 0;
        let limit = 40; // Mastodon API limit; default 20, max 40
        let mut trends = Vec::new();
        loop {
            let mut params = HashMap::new();
            params.insert("offset", offset.to_string());
            params.insert("limit", limit.to_string());
            let parsed_uri = Url::parse_with_params(&url, &params);
            if parsed_uri.is_err() {
                error!(
                    "Error parsing URI: {} on {}",
                    parsed_uri.expect_err("Trending statuses error"),
                    url.red()
                );
                break;
            }
            let response = client.get(parsed_uri.expect("a parsed url should be valid uri")).timeout(Duration::from_secs(30)).send().await;
            if response.is_err() {
                error!(
                    "Error HTTP: {} on {}",
                    response.expect_err("Trending statuses error"),
                    url.red()
                );
                break;
            }
            let response = response.expect("should be trending statuses");
            if !response.status().is_success() {
                error!("Error HTTP: {} on {}", response.status(), url.red());
                break;
            }
            let json = response.text().await.expect("should be trending statuses");
            debug!("Reading Trending JSON from {}", url);
            let trending_statuses_raw: std::prelude::v1::Result<Vec<Status>, serde_json::Error> =
                serde_json::from_str(&json);
            if trending_statuses_raw.is_err() {
                warn!("Parsing Issue on Trending JSON");
                let error = trending_statuses_raw.expect_err("Trending statuses error");
                error!("Error JSON: {} on {}", error, url.red());
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
        let fetch_timer_end = std::time::Instant::now();
        let fetch_timer_duration = fetch_timer_end - fetch_timer_start;
        info!(
            "Fetched {} trending statuses in {} seconds from {base}",
            trends.len(),
            fetch_timer_duration.as_secs(),
            base = base
        );
        Ok(trends)
    }

    /// Find the status ID for a given URI from the home instance's database.
    #[async_recursion]
    pub async fn find_status_id(
        status: &Status,
        pool: &PgPool,
        home_instance_url: &String,
        home_instance: &Instance,
        rocket_hostname: &String,
    ) -> Result<i64> {
        debug!("Finding status ID for {uri}", uri = status.uri);
        let client = home_instance.client();
        let home_instance_token = home_instance.token().expect("should be token");
        let uri = status.uri.to_string();
        let select_statement = sqlx::query!(r#"SELECT id FROM statuses WHERE uri = $1"#, uri)
            .fetch_one(pool)
            .await;

        if let Ok(status_record) = select_statement {
            debug!("Status found in database: {uri}", uri = uri);
            Ok(status_record.id)
        } else {
            debug!("Status not found in database, searching for it: {uri}");
            if status.in_reply_to_id.is_some() {
                debug!("Status is a reply, skipping: {}", &status.uri);
                return Err(mastodon_async::Error::Other(
                    "Status is a reply".to_string(),
                ));
            }
            let status_id_from_uri = status
                .uri
                .as_ref()
                .split('/')
                .last()
                .expect("should be Status ID")
                .to_string();
            let user_name_without_domain = status
                .account
                .acct
                .split('@')
                .next()
                .expect("should be user name");
            let replacement_uri = format!("https://{rocket_hostname}.shatteredsky.net/users/{user_name_without_domain}/statuses/{status_id_from_uri}");
            let search_url = format!(
                "https://{home_instance_url}/api/v2/search?q={replacement_uri}&resolve=true"
            );
            let parsed_search_url = Url::parse(&search_url);
            if parsed_search_url.is_err() {
                error!(
                    "Error parsing URI: {} on {}",
                    parsed_search_url.expect_err("Search for Status"),
                    search_url.red()
                );
                return Err(mastodon_async::Error::Other(
                    "Search for Status".to_string(),
                ));
            }
            debug!("Searching for status: {uri}", uri = parsed_search_url.expect("a parsed url should be valid uri"));
            let search_result = client
                .get(&search_url)
                .timeout(Duration::from_secs(30))
                .bearer_auth(home_instance_token)
                .send()
                .await;
            if search_result.is_err() {
                let received_error = search_result.expect_err("should be error");
                error!("Error result: {}", received_error);
                return Err(mastodon_async::Error::Other(
                    "Search for Status".to_string(),
                ));
            }
            let search_result = search_result.expect("should be search result");
            if !search_result.status().is_success() {
                if (search_result.status().as_u16() == 429)
                    || (search_result.status().to_string().contains("429"))
                {
                    let retry_after = search_result
                        .headers()
                        .get("x-ratelimit-reset")
                        .expect("should be x-ratelimit-reset")
                        .to_str()
                        .expect("should be str")
                        .parse::<chrono::DateTime<chrono::Utc>>()
                        .expect("should be chrono::DateTime<chrono::Utc>");
                    let now = chrono::Utc::now();
                    let duration = retry_after - now;
                    let duration = duration.to_std().expect("should be std::time::Duration");
                    info!(
                        "Sleeping for {duration} seconds for {uri}",
                        duration = duration.as_secs(),
                        uri = uri
                    );
                    tokio::time::sleep(duration).await;
                    return Self::find_status_id(status, pool, home_instance_url, home_instance, rocket_hostname)
                        .await;
                }
                error!("Error HTTP: {}", search_result.status());
                return Err(mastodon_async::Error::Other(
                    "Search for Status".to_string(),
                ));
            }
            debug!("Search success for status: {uri}", uri = uri);
            let search_result = search_result.text().await.expect("should be search result");
            let search_result_result = serde_json::from_str::<SearchResult>(&search_result);
            if let Err(err) = search_result_result {
                error!("Error JSON: {} on {}", err, search_url);
                json_window(&err, &search_result);
                return Err(mastodon_async::Error::Other(
                    "Search for Status".to_string(),
                ));
            }
            let search_result_result_result: SearchResult =
                search_result_result.expect("should be search result");
            if search_result_result_result.statuses.is_empty() {
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
                    |status_record| Ok(status_record.id),
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
        home_server_url: &String,
        home_server_instance: &Instance,
        rocket_hostname: &String,
        instance_collection: &HashMap<String, Instance>,
    ) -> Result<Option<HashMap<String, Status>>> {
        debug!("Status: {}", &status.uri);
        let original_id_string = &status
            .uri
            .as_ref()
            .split('/')
            .last()
            .expect("should be Status ID")
            .to_string();
        if !original_id_string.chars().all(char::is_numeric) {
            warn!("ID is not numeric, skipping: {}", &status.uri);
            return Ok(None);
        }
        let mut additional_context_statuses = HashMap::new();
        let status_id =
            Self::find_status_id(status, pool, home_server_url, home_server_instance, rocket_hostname).await;
        if status_id.is_err() {
            warn!(
                "Status not found by home server, skipping: {} , Error: {}",
                &status.uri,
                status_id.expect_err("should be error")
            );
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

    pub async fn start_rocket(statuses: HashMap<String, Status>) {
        let statuses = statuses
            .values()
            .map(convert_status_to_activitypub)
            .collect::<Vec<_>>();
        let state = AppState(statuses);
        #[allow(clippy::no_effect_underscore_binding)]
        let _ = rocket::build()
            .mount("/", rocket::routes![search, shutdown])
            .manage(state)
            .launch()
            .await;
    }
}

async fn update_status(
    reblogs_count: i64,
    replies_count: i64,
    favourites_count: i64,
    status: &Status,
    status_id: i64,
    pool: &sqlx::Pool<sqlx::Postgres>,
    instance_collection: &HashMap<String, Instance>,
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
            debug!("Error fetching descendants");
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
    instance_collection: &HashMap<String, Instance>,
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
        error!("Error inserting status {} : {err}", &status.uri);
        return None;
    }
    if status.replies_count > 0 {
        let descendants = get_status_descendants(status, instance_collection).await;
        if descendants.is_none() {
            debug!("No descendants fetched for status: {}", &status.uri);
            return None;
        }
        let descendants = descendants.expect("should be descendants");
        return Some(descendants);
    }
    None
}

#[allow(unreachable_code, unused_variables)]
async fn get_status_descendants(
    status: &Status,
    instance_collection: &HashMap<String, Instance>,
) -> Option<Vec<Status>> {
    return None; // TODO: Temporarily disables fetching descendants while we work on the API
    debug!("Fetching context for status: {}", &status.uri);
    let original_id = StatusId::new(
        status
            .uri
            .as_ref()
            .split('/')
            .last()
            .expect("should be Status ID")
            .to_string(),
    );
    let original_id_string = &status
        .uri
        .as_ref()
        .split('/')
        .last()
        .expect("should be Status ID")
        .to_string();
    if !original_id_string.chars().all(char::is_numeric) {
        warn!("Original ID is not numeric, skipping: {original_id}");
        return None;
    }
    debug!("Original ID: {original_id}");
    let base_server = reqwest::Url::parse(status.uri.as_ref())
        .expect("should be Status URI")
        .host_str()
        .expect("should be base server string")
        .to_string();
    let context = get_status_context(instance_collection, base_server, original_id).await?;
    debug!(
        "Fetched context for status: {}, ancestors: {}, descendants: {}",
        &status.uri,
        context.ancestors.len(),
        context.descendants.len()
    );
    Some(context.descendants)
}

#[async_recursion]
async fn get_status_context(
    instance_collection: &HashMap<String, Instance>,
    base_server: String,
    original_id: StatusId,
) -> Option<Context> {
    let original_id_string = &original_id.to_string();
    let url_string = format!("https://{base_server}/api/v1/statuses/{original_id_string}/context");
    let url = Url::parse(&url_string);
    if url.is_err() {
        error!(
            "Error parsing URI: {} on {}",
            url.expect_err("Context of Status"),
            url_string.red()
        );
        return None;
    }
    let url = url.expect("should be url");

    let instance = instance_collection
        .get(&base_server)
        .expect("should be instance");
    let client = instance.client();
    if let Some(instance_token) = instance.token() {
        let response = client
            .get(url.clone())
            .timeout(Duration::from_secs(30))
            .bearer_auth(instance_token)
            .send()
            .await;
        if response.is_err() {
            error!(
                "{} {} from {}",
                "Error result:".red(),
                response.expect_err("Context of Status"),
                &url
            );
            return None;
        }
        let response = response.expect("should be Context of Status");
        if !response.status().is_success() {
            if response.status().as_u16() == 429 {
                let retry_after = response
                    .headers()
                    .get("x-ratelimit-reset")
                    .expect("should be x-ratelimit-reset")
                    .to_str()
                    .expect("should be str")
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .expect("should be chrono::DateTime<chrono::Utc>");
                let now = chrono::Utc::now();
                let duration = retry_after - now;
                let duration = duration.to_std().expect("should be std::time::Duration");
                info!(
                    "Sleeping for {duration} seconds for {uri}",
                    duration = duration.as_secs(),
                    uri = &url
                );
                tokio::time::sleep(duration).await;
                return get_status_context(instance_collection, base_server, original_id).await;
            }
            error!(
                "{} {} from {}",
                "Error HTTP:".red(),
                response.status(),
                &url
            );
            return None;
        }
        let json = response.text().await.expect("should be Context of Status");
        let context = serde_json::from_str::<Context>(&json);
        if let Err(err) = context {
            error!("{} {err}", "Error JSON:".red());
            json_window(&err, &json);
            return None;
        }
        return Some(context.expect("should be Context of Status"));
    }
    let response = client.get(url.clone()).timeout(Duration::from_secs(30)).send().await;
    if response.is_err() {
        error!(
            "{} {} from {}",
            "Error result:".red(),
            response.expect_err("Context of Status"),
            &url
        );
        return None;
    }
    let response = response.expect("should be Context of Status");
    if !response.status().is_success() {
        if response.status().as_u16() == 429 {
            let retry_after = response
                .headers()
                .get("x-ratelimit-reset")
                .expect("should be x-ratelimit-reset")
                .to_str()
                .expect("should be str")
                .parse::<chrono::DateTime<chrono::Utc>>()
                .expect("should be chrono::DateTime<chrono::Utc>");
            let now = chrono::Utc::now();
            let duration = retry_after - now;
            let duration = duration.to_std().expect("should be std::time::Duration");
            info!(
                "Sleeping for {duration} seconds for {uri}",
                duration = duration.as_secs(),
                uri = &url
            );
            tokio::time::sleep(duration).await;
            return get_status_context(instance_collection, base_server, original_id).await;
        }
        error!(
            "{} {} from {}",
            "Error HTTP:".red(),
            response.status(),
            &url
        );
        return None;
    }
    let json = response.text().await.expect("should be Context of Status");
    let context = serde_json::from_str::<Context>(&json);
    if let Err(err) = context {
        error!("{} {err}", "Error JSON:".red());
        json_window(&err, &json);
        return None;
    }
    Some(context.expect("should be Context of Status"))
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
    for _ in 0..500 {
        if start == 0 {
            break;
        }
        start -= 1;
    }
    for _ in 0..500 {
        if end == json.len() {
            break;
        }
        end += 1;
    }
    let json = String::from_utf8_lossy(&json[start..end]);
    let json = json.replace(
        &json[column - start..column - start + 5],
        &json[column - start..column - start + 5].red().to_string(),
    );
    error!("Error JSON preview window: {}", json);
    // wait until 'enter' is pressed
    // let mut input = String::new();
    // std::io::stdin()
    //     .read_line(&mut input)
    //     .expect("should be able to read line");
    //     fn invalid_type(unexp: Unexpected<'_>, exp: &dyn Expected) -> Self
}
