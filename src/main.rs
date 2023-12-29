// src/main.rs

use std::collections::{HashMap, HashSet};
use std::env;

use mastodon_async::prelude::*;
use mastodon_async::{helpers::cli, Result};

use sqlx::postgres::PgPool;
use toml::Table;

const ONE_PAGE: usize = 40;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let config_string = &std::fs::read_to_string("config.toml")?;
    let config: Table = toml::from_str(config_string).unwrap();
    let home: &str = config.get("servers").unwrap().get("home").unwrap().as_str().unwrap();
    let instance_collection: HashMap<String, Mastodon> = HashMap::new();

    let authenticated_strings = config.get("servers").unwrap().get("authenticated").unwrap().as_array().unwrap();
    // let unauthenticated_strings = config.get("servers").unwrap().get("unauthenticated").unwrap().as_array().unwrap();
    let mut instance_collection = Fed::get_instance(instance_collection, home).await?;
    for server in authenticated_strings {
        if !instance_collection.contains_key(server.as_str().unwrap()) {
            instance_collection = Fed::get_instance(instance_collection, server.as_str().unwrap()).await?;
        }
    }

    let mut queued_servers: HashSet<String> = HashSet::new();
    
    let mut statuses = HashMap::new();
    println!("\x1b[32mFetching trending statuses\x1b[0m");
    for (_, remote) in instance_collection.iter() {
        for status in Fed::fetch_trending_statuses(&remote.data.base, ONE_PAGE).await? {
            if status.uri.contains(&remote.data.base as &str) {
                println!("Status: {}", status.uri);
            } else {
                println!("Status from another server: {}", status.uri);
                let base = status.uri.split('/').nth(2).unwrap();
                if !instance_collection.contains_key(base) {
                    // It's not in the instance collection, but it might already be queued
                    if queued_servers.insert(base.to_string()) {
                        println!("Queued server: {}", base);
                    }
                }
            }
            statuses.entry(status.uri.clone()).and_modify(|existing_status: &mut Status| {
                println!("Duplicate status, Reb: {:?}, Rep: {:?}, Fav: {:?}", existing_status.reblogs_count, existing_status.replies_count.unwrap_or(0), existing_status.favourites_count);
                existing_status.reblogs_count = std::cmp::max(existing_status.reblogs_count, status.reblogs_count);
                existing_status.replies_count = std::cmp::max(existing_status.replies_count, status.replies_count);
                existing_status.favourites_count = std::cmp::max(existing_status.favourites_count, status.favourites_count);
            }).or_insert(status);
        }
    }
    println!("\x1b[32mTotal statuses\x1b[0m: {}", statuses.len());
    println!("\x1b[32mQueued servers\x1b[0m: {}", queued_servers.len());
    println!("\x1b[32mFetching trending statuses from queued servers\x1b[0m");
    for server in queued_servers {
        for status in Fed::fetch_trending_statuses(&server, ONE_PAGE).await? {
            statuses.entry(status.uri.clone()).and_modify(|existing_status: &mut Status| {
                println!("Duplicate status, Reb: {:?}, Rep: {:?}, Fav: {:?}", existing_status.reblogs_count, existing_status.replies_count.unwrap_or(0), existing_status.favourites_count);
                existing_status.reblogs_count = std::cmp::max(existing_status.reblogs_count, status.reblogs_count);
                existing_status.replies_count = std::cmp::max(existing_status.replies_count, status.replies_count);
                existing_status.favourites_count = std::cmp::max(existing_status.favourites_count, status.favourites_count);
            }).or_insert(status);
        }
    }
    for (uri, status) in &statuses {
        println!("{}: {:?} reblogs, {:?} replies, {:?} favourites", uri, status.reblogs_count, status.replies_count.unwrap_or(0), status.favourites_count);
    }

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&database_url).await.unwrap();

    println!("\x1b[32mInserting or updating statuses\x1b[0m");
    let mut context_of_statuses = HashMap::new();
    for (uri, status) in statuses {
        println!("Status: {}", uri);
        match Fed::find_status_id(&uri, &pool, &instance_collection, home).await {
            Ok(status_id) => {
                if status.reblogs_count == 0 && status.replies_count.unwrap_or(0) == 0 && status.favourites_count == 0 {
                    println!("Status has no interactions, skipping: {}", uri);
                    continue;
                }
                println!("Status found in database: {}", uri);
                let select_statement = sqlx::query!(
                    r#"SELECT id, reblogs_count, replies_count, favourites_count FROM status_stats WHERE status_id = $1"#,
                    status_id
                );
                let select_statement = select_statement.fetch_one(&pool
                ).await;
        
                if select_statement.is_err() {
                    println!("Status not found in status_stats table, inserting it: {}", uri);
                    let offset_date_time = time::OffsetDateTime::now_utc();
                    let current_time = time::PrimitiveDateTime::new(offset_date_time.date(), offset_date_time.time());
                    let insert_statement = sqlx::query!(
                        r#"INSERT INTO status_stats (status_id, reblogs_count, replies_count, favourites_count, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6)"#,
                        status_id,
                        status.reblogs_count as i64,
                        status.replies_count.unwrap_or(0) as i64,
                        status.favourites_count as i64,
                        current_time,
                        current_time
                    );
                    let insert_statement = insert_statement.execute(&pool).await;
                    if insert_statement.is_err() {
                        println!("Error inserting status: {}", insert_statement.err().unwrap());
                        continue;
                    }
                    if status.replies_count.unwrap_or(0) > 0 {
                        println!("Fetching context for status: {}", uri);
                        let original_id = StatusId::new(uri.split('/').last().unwrap().to_string());
                        let original_id_string = uri.split('/').last().unwrap().to_string();
                        println!("Original ID: {}", original_id);
                        let base_server = reqwest::Url::parse(&uri)?.host_str().unwrap().to_string();
                        if instance_collection.contains_key(&base_server) {
                            println!("Fetching context for status from instance collection: {}", uri);
                            let remote = instance_collection.get(&base_server).unwrap();
                            let context = remote.get_context(&original_id).await?;
                            for ancestor_status in context.ancestors {
                                context_of_statuses.entry(ancestor_status.uri.clone()).or_insert(ancestor_status);
                            }
                            for descendant_status in context.descendants {
                                context_of_statuses.entry(descendant_status.uri.clone()).or_insert(descendant_status);
                            }
                        } else {
                            println!("Fetching context for status from new instance: {}", uri);
                            println!("Registering unauth instance: {}", base_server);
                            let url = format!("https://{}/api/v1/statuses/{}/context", base_server, original_id_string);
                            let response = reqwest::Client::new().get(&url).send().await?;
                            if !response.status().is_success() {
                                println!("Error HTTP: {}", response.status());
                                continue;
                            }
                            let json = response.text().await?;
                            let context = serde_json::from_str::<Context>(&json);
                            if context.is_err() {
                                println!("Error JSON: {}", context.err().unwrap());
                                continue;
                            }
                            let context = context.unwrap();
                            for ancestor_status in context.ancestors {
                                context_of_statuses.entry(ancestor_status.uri.clone()).or_insert(ancestor_status);
                            }
                            for descendant_status in context.descendants {
                                context_of_statuses.entry(descendant_status.uri.clone()).or_insert(descendant_status);
                            }
                        }
                    }
                } else {
                    let unwrapped = select_statement.unwrap();
                    let old_reblogs_count = unwrapped.reblogs_count as u64;
                    let old_replies_count = unwrapped.replies_count as u64;
                    let old_favourites_count = unwrapped.favourites_count as u64;
                    if (status.reblogs_count <= old_reblogs_count) && (status.replies_count.unwrap_or(0) <= old_replies_count) && (status.favourites_count <= old_favourites_count) {
                        println!("Status found in status_stats table, but we have smaller counts, skipping: {}", uri);
                        continue;
                    }
                    println!("Status found in status_stats table, updating it: {}", uri);
                    let offset_date_time = time::OffsetDateTime::now_utc();
                    let current_time = time::PrimitiveDateTime::new(offset_date_time.date(), offset_date_time.time());
                    let update_statement = sqlx::query!(
                        r#"UPDATE status_stats SET reblogs_count = $1, replies_count = $2, favourites_count = $3, updated_at = $4 WHERE status_id = $5"#,
                        status.reblogs_count as i64,
                        status.replies_count.unwrap_or(0) as i64,
                        status.favourites_count as i64,
                        current_time,
                        status_id
                    );
                    let update_statement = update_statement.execute(&pool).await;
                    if update_statement.is_err() {
                        println!("Error updating status: {}", update_statement.err().unwrap());
                        continue;
                    }
                    if old_replies_count < status.replies_count.unwrap_or(0) {
                        println!("Fetching context for status: {}", uri);
                        let original_id = StatusId::new(uri.split('/').last().unwrap().to_string());
                        let original_id_string = uri.split('/').last().unwrap().to_string();
                        println!("Original ID: {}", original_id);
                        let base_server = reqwest::Url::parse(&uri)?.host_str().unwrap().to_string();
                        if instance_collection.contains_key(&base_server) {
                            println!("Fetching context for status from instance collection: {}", uri);
                            let remote = instance_collection.get(&base_server).unwrap();
                            let context = remote.get_context(&original_id).await?;
                            for ancestor_status in context.ancestors {
                                context_of_statuses.entry(ancestor_status.uri.clone()).or_insert(ancestor_status);
                            }
                            for descendant_status in context.descendants {
                                context_of_statuses.entry(descendant_status.uri.clone()).or_insert(descendant_status);
                            }
                        } else {
                            println!("Fetching context for status from new instance: {}", uri);
                            println!("Registering unauth instance: {}", base_server);
                            let url = format!("https://{}/api/v1/statuses/{}/context", base_server, original_id_string);
                            let response = reqwest::Client::new().get(&url).send().await?;
                            if !response.status().is_success() {
                                println!("Error HTTP: {}", response.status());
                                continue;
                            }
                            let json = response.text().await?;
                            let context = serde_json::from_str::<Context>(&json);
                            if context.is_err() {
                                println!("Error JSON: {}", context.err().unwrap());
                                continue;
                            }
                            let context = context.unwrap();
                            for ancestor_status in context.ancestors {
                                context_of_statuses.entry(ancestor_status.uri.clone()).or_insert(ancestor_status);
                            }
                            for descendant_status in context.descendants {
                                context_of_statuses.entry(descendant_status.uri.clone()).or_insert(descendant_status);
                            }
                        }
                    }
                }
            },
            Err(_) => {
                println!("Status not found in database: {}", uri);
                continue;
            }
        }
    println!("\x1b[32mFetched {} OK!\x1b[0m", uri);
    }

    println!("\x1b[32mFetching context statuses\x1b[0m");
    for status in context_of_statuses {
        if status.1.reblogs_count == 0 && status.1.replies_count.unwrap_or(0) == 0 && status.1.favourites_count == 0 {
            println!("Status has no interactions, skipping: {}", status.0);
            continue;
        }
        match Fed::find_status_id(&status.0, &pool, &instance_collection, home).await {
            Ok(status_id) => {
                println!("Status found in database: {}", status.0);
                let select_statement = sqlx::query!(
                    r#"SELECT id, reblogs_count, replies_count, favourites_count FROM status_stats WHERE status_id = $1"#,
                    status_id
                );
                let select_statement = select_statement.fetch_one(&pool
                ).await;
        
                if select_statement.is_err() {
                    println!("Status not found in status_stats table, inserting it: {}", status.0);
                    let offset_date_time = time::OffsetDateTime::now_utc();
                    let current_time = time::PrimitiveDateTime::new(offset_date_time.date(), offset_date_time.time());
                    let insert_statement = sqlx::query!(
                        r#"INSERT INTO status_stats (status_id, reblogs_count, replies_count, favourites_count, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6)"#,
                        status_id,
                        status.1.reblogs_count as i64,
                        status.1.replies_count.unwrap_or(0) as i64,
                        status.1.favourites_count as i64,
                        current_time,
                        current_time
                    );
                    let insert_statement = insert_statement.execute(&pool).await;
                    if insert_statement.is_err() {
                        println!("Error inserting status: {}", insert_statement.err().unwrap());
                        continue;
                    }
                } else {
                    let unwrapped = select_statement.unwrap();
                    let old_reblogs_count = unwrapped.reblogs_count as u64;
                    let old_replies_count = unwrapped.replies_count as u64;
                    let old_favourites_count = unwrapped.favourites_count as u64;
                    if (status.1.reblogs_count <= old_reblogs_count) && (status.1.replies_count.unwrap_or(0) <= old_replies_count) && (status.1.favourites_count <= old_favourites_count) {
                        println!("Status found in status_stats table, but we have smaller counts, skipping: {}", status.0);
                        continue;
                    }
                    println!("Status found in status_stats table, updating it: {}", status.0);
                    let offset_date_time = time::OffsetDateTime::now_utc();
                    let current_time = time::PrimitiveDateTime::new(offset_date_time.date(), offset_date_time.time());
                    let update_statement = sqlx::query!(
                        r#"UPDATE status_stats SET reblogs_count = $1, replies_count = $2, favourites_count = $3, updated_at = $4 WHERE status_id = $5"#,
                        status.1.reblogs_count as i64,
                        status.1.replies_count.unwrap_or(0) as i64,
                        status.1.favourites_count as i64,
                        current_time,
                        status_id
                    );
                    let update_statement = update_statement.execute(&pool).await;
                    if update_statement.is_err() {
                        println!("Error updating status: {}", update_statement.err().unwrap());
                        continue;
                    }
                }
                println!("\x1b[32mStatus {} found OK!\x1b[0m", status.0);
            },
            Err(_) => {
                println!("Status not found in database: {}", status.0);
                continue;
            }
        }
    }

    println!("\x1b[32mAll OK!\x1b[0m");
    Ok(())
}

pub struct Fed;

impl Fed {
    async fn register(server: &str) -> Result<Mastodon> {
        if let Ok(data) = mastodon_async::helpers::toml::from_file(format!("federation/{}-data.toml", server)) {
            println!("Using cached data for {}", server);
            return Ok(Mastodon::from(data));
        }
        println!("First time registration with {}", server);
        let url = format!("https://{}", server);
        let registration = Registration::new(url)
                                        .client_name("mastodon-async-examples")
                                        .build()
                                        .await?;
        let mastodon = cli::authenticate(registration).await?;
        mastodon_async::helpers::toml::to_file(&mastodon.data, format!("federation/{}-data.toml", server))?;
        Ok(mastodon)
    }

    pub async fn get_instance(instance_collection: HashMap<String, Mastodon>, server: &str) -> Result<HashMap<String, Mastodon>> {
        let mut instance_collection = instance_collection;
        let server = server.strip_prefix("https://").unwrap_or(server);
        if instance_collection.contains_key(server) {
            println!("Instance already registered: {}", server);
        } else {
            println!("Registering instance: {}", server);
            let instance = Fed::register(server).await?;
            instance_collection.insert(server.to_string(), instance.clone());
        }
        Ok(instance_collection)
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

    pub async fn find_status_id(uri: &str, pool: &PgPool, instance_collection: &HashMap<String, Mastodon>, home: &str) -> Result<i64> {
        let select_statement = sqlx::query!(
            r#"SELECT id FROM statuses WHERE uri = $1"#,
            uri
        ).fetch_one(pool).await;
    
        match select_statement {
            Ok(status) => Ok(status.id),
            Err(_) => {
                println!("Status not found in database, searching for it: {}", uri);
                let search_result = instance_collection.get(home).unwrap().search(uri, true).await?;
                if search_result.statuses.is_empty() {
                    let message: String = format!("Status not found by home server: {}", uri);
                    return Err(mastodon_async::Error::Other(message));
                }
    
                let select_statement = sqlx::query!(
                    r#"SELECT id FROM statuses WHERE uri = $1"#,
                    uri
                ).fetch_one(pool).await;
    
                match select_statement {
                    Ok(status) => Ok(status.id),
                    Err(_) => {
                        println!("Status still not found in database, giving up: {}", uri);
                        let message: String = format!("Status not found in database: {}", uri);
                        Err(mastodon_async::Error::Other(message))
                    }
                }
            }
        }
    }
}