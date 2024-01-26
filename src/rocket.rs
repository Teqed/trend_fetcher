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
)]


use colored::Colorize;
use std::collections::HashMap;
use std::error::Error;
use mastodon_async::prelude::*;
use reqwest::Url;
use reqwest_middleware::ClientBuilder;
use reqwest_retry_after::RetryAfterMiddleware;
use color_eyre::eyre::Result;

use tracing::{debug, error, info, warn};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
struct Account {
    id: String,
    url: String,
}

#[derive(Debug, Deserialize)]
struct MediaAttachment {
    id: String,
    url: String,
    remote_url: Option<String>,
    preview_url: String,
    meta: Option<AttachmentMeta>,
}

#[derive(Debug, Deserialize)]
struct AttachmentMeta {
    original: Option<MediaMeta>,
}

#[derive(Debug, Deserialize)]
struct MediaMeta {
    width: Option<i32>,
    height: Option<i32>,
    frame_rate: Option<String>,
    duration: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct Tag {
    name: String,
    url: String,
}

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

const PAGE: usize = 40;

fn convert_status_to_activitypub(status: &Status) -> ActivityPubNote {
    let media_attachments: Vec<APAttachment> = status.media_attachments.iter().map(|media| {
        let meta = media.meta.as_ref().and_then(|m| m.original.as_ref());
        let media_clone = media.url.clone().expect("should be url").to_string();
        let extension = media_clone.split('.').last().expect("should be extension");
        let type_from_media = match media.media_type {
            MediaType::Image => {
                match extension {
                    "png" => "image/png".to_string(),
                    "jpg" | "jpeg" => "image/jpeg".to_string(),
                    "gif" => "image/gif".to_string(),
                    _ => "image".to_string(),
                }
            }
            MediaType::Video | MediaType::Gifv => {
                match extension {
                    "mp4" => "video/mp4".to_string(),
                    "webm" => "video/webm".to_string(),
                    _ => "video".to_string(),
                }
            }
            MediaType::Audio => {
                let media_clone = media.url.clone().expect("should be url").to_string();
                let extension = media_clone.split('.').last().expect("should be extension");
                match extension {
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
            url: media.remote_url.clone().unwrap_or(media.url.clone().expect("should be url")).to_string(),
            name: media.description.clone().map(Value::String),
            blurhash: media.blurhash.clone().unwrap_or_default(),
            focal_point: vec![0, 0],
            width: meta.and_then(|m| m.width),
            height: meta.and_then(|m| m.height),
        }
    }).collect();

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
    let content_map = serde_json::Map::from_iter(vec![(language_code.clone().expect("should be language code"), Value::String("Test String".to_string()))]);

    ActivityPubNote {
        context,
        id: status.uri.clone(),
        type_: "Note".to_string(),
        summary: None,
        in_reply_to: None,
        published: status.created_at.to_string(),
        url: status.url.clone().expect("should be url"),
        attributed_to: Some(status.account.uri.clone().to_string()),
        to: vec!["https://www.w3.org/ns/activitystreams#Public".to_string()],
        cc: vec![format!("{}/followers", status.account.uri)],
        sensitive: status.sensitive,
        atom_uri: Some(status.uri.clone()),
        in_reply_to_atom_uri: None,
        conversation: String::new(),
        content: "Test String".to_string(),
        content_map,
        attachment: media_attachments,
        tag: status.tags.iter().map(|tag| APTag {
            type_: "Hashtag".to_string(),
            href: tag.url.clone(),
            name: format!("#{}", tag.name),
        }).collect(),
        replies: APReplies {
            id: format!("{}/replies", status.uri.clone()),
            type_: "Collection".to_string(),
            first: APFirst {
                type_: "CollectionPage".to_string(),
                next: format!("{}/replies?only_other_accounts=true&page=true", status.uri.clone()),
                part_of: Some(format!("{}/replies", status.uri.clone())),
                items: Vec::new(),
            },
        },
    }
}

struct AppState(Vec<ActivityPubNote>);

// We want to use the LINK header
use rocket::request::Request;
use rocket::response::{self, Response, Responder};
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

#[rocket::get("/users/<user>/statuses/<status_id>")]
fn search(user: &str, status_id: &str, route_json: &rocket::State<AppState>) -> ActivityJsonResponder {
    println!("Got request for user {} and status {}", user, status_id);
    // let status = route_json.0.iter().find(|status| status.id.path() == format!("/@{}/{}", user, status_id)).expect("should be status");
    let status = route_json.0.iter().find(|status| status.id.path() == format!("/users/{}/statuses/{}", user, status_id)).expect("should be status");
    for status in &route_json.0 {
        println!("Found status: {}", status.id.path());
    }
    let status = serde_json::to_value(status).expect("should be valid json");
    let status: ActivityPubNote = serde_json::from_value(status).expect("should be valid status");
    println!("Found status: {}", status.id.path());
    println!("Responding with JSON: {}", serde_json::to_string_pretty(&status).expect("should be valid json"));
    ActivityJsonResponder {
        inner: Json(status),
    }
}

#[tokio::main]
#[tracing::instrument]
async fn main() -> Result<(), Box<dyn Error>> {
    // let accept_header = "application/activity+json, application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\", text/html;q=0.1";
    // let accept_header_reqwest = reqwest::header::HeaderValue::from_str(accept_header).expect("should be valid header value");
    // let client = reqwest::Client::builder()
    //     .user_agent("Mastodon API Client")
    //     .default_headers({
    //         let mut headers = reqwest::header::HeaderMap::new();
    //         headers.insert(reqwest::header::ACCEPT, accept_header_reqwest);
    //         headers
    //     })
    //     .build()
    //     .expect("should be valid client");
    // let url = "https://mastodon.shatteredsky.net/users/teq/statuses/109612104811129202";
    // let request = client.get(url);
    // let response = request.send().await.expect("should be valid response");
    // let activity_pub_json = response.json().await.expect("should be valid json");
    // println!("Got JSON: {}", serde_json::to_string_pretty(&activity_pub_json).expect("should be valid json"));
    // let activity_pub_json: ActivityPubNote = serde_json::from_value(activity_pub_json).expect("should be valid status");
    // let trends = vec![activity_pub_json];
    let trends = fetch_trending_statuses("universeodon.com").await.expect("should be valid trends");
    let trends = trends.iter().map(convert_status_to_activitypub).collect::<Vec<_>>();
    for status in &trends {
        println!("Found status: {}", status.id.path());
    }

    let rocket = rocket::build()
        .manage(AppState(trends))
        .mount("/", rocket::routes![search]);
    rocket.launch().await.expect("should be valid launch");

    return Ok(());
}

async fn fetch_trending_statuses(base: &str) -> Result<Vec<Status>> {
    info!("Fetching trending statuses from {base}");
    let base = base.strip_prefix("https://").unwrap_or(base);
    let url = format!("https://{base}/api/v1/trends/statuses");
    let mut offset = 0;
    let limit = 10; // Mastodon API limit; default 20, max 40
    let mut trends = Vec::new();
    loop {
        let mut params = HashMap::new();
        params.insert("offset", offset.to_string());
        params.insert("limit", limit.to_string());
        let response = ClientBuilder::new(reqwest::Client::new())
            .with(RetryAfterMiddleware::new())
            .build()
            .get(&url).query(&params).send().await;
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
    Ok(trends)
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
