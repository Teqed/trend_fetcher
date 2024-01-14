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
    clippy::question_mark_used,
    // restrictions
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

use std::collections::{HashMap, HashSet};
use colored::Colorize;
use federation::Federation;
use futures::stream::{self, StreamExt};
use sqlx::postgres::PgPool;
use tracing::{debug, error, info, warn};

/// The federation module provides functions for interacting with the Mastodon API.
mod federation;
/// The configuration module provides functions for loading the configuration.
mod configuration;

/// The number of statuses to fetch per page. This is the maximum number of statuses that can be fetched per page, defined by the Mastodon API.
const PAGE: usize = 40;
/// The maximum number of futures to run concurrently.
const MAX_FUTURES: usize = 15;

/// Processes an iterator concurrently.
/// 
/// # Arguments
/// 
/// * `iter` - The iterator to process.
/// * `processor` - The function to process each item in the iterator.
/// * `limit` - The maximum number of futures to run concurrently.
/// 
/// # Returns
/// 
/// * `Vec<T>` - The results of processing the iterator.
async fn process_concurrently<I, TI, TO, E, F, Fut>(iter: I, processor: F, limit: usize) -> Result<Vec<TO>, E>
where
    I: IntoIterator<Item = TI> + Send, // Add the Send trait to I.
    I::IntoIter: Send, // Add the Send trait to the associated type IntoIter of IntoIterator.
    TI: Send + 'static, // Type of items in the iterator.
    TO: Send + 'static, // Type of output items that the processing function returns.
    F: Fn(TI) -> Fut + Copy + Send, // Add the Send trait to F.
    Fut: std::future::Future<Output = Result<TO, E>> + Send,
    E: std::fmt::Debug + Send + 'static,
{
    stream::iter(iter)
        .map(processor)
        .buffer_unordered(limit)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>() // Collect results, handling errors
}

#[tokio::main]
#[tracing::instrument]
/// The main function.
async fn main() -> Result<(), ()> {
    debug!("Starting");
    if false { error!("This is an error"); }
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).expect("should be default subscriber");
    let start = time::OffsetDateTime::now_utc();
    let mut instance_collection = HashMap::new();
    let config = configuration::load_config();
    let home_server =
        Federation::get_instance(&mut instance_collection, &config.servers.home).await;
    for server in config.servers.authenticated {
        Federation::get_instance(&mut instance_collection, &server).await;
    }
    let mut queued_servers: HashSet<String> = HashSet::new();
    for server in config.servers.unauthenticated {
        queued_servers.insert(server);
    }
    info!("{}", "Fetching trending statuses".green().to_string());
    let trending_statuses = process_concurrently(
        instance_collection.keys().cloned(), // Clone the keys (base URLs) from the instance_collection.
        |base| {
            let base_clone = base; // Clone base to move into the async block.
            async move {
                Federation::fetch_trending_statuses(&base_clone, PAGE).await
            }
        },
        MAX_FUTURES,
    )
    .await
    .expect("Error fetching trending statuses");
    let mut trending_statuses_hashmap = {
        let mut trending_statuses_hashmap = HashMap::new();
        for status in trending_statuses.into_iter().flatten() {
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
            Federation::modify_counts(&mut trending_statuses_hashmap, status);
        }
        trending_statuses_hashmap
    };


    info!("Total statuses: {}", trending_statuses_hashmap.len());
    info!("Queued servers: {}", queued_servers.len());
    info!(
        "{}",
        "Fetching trending statuses from queued servers".green()
    );
    let aux_trending_statuses = process_concurrently(
        queued_servers.iter().cloned(),
        |server| {
            async move {
                Federation::fetch_trending_statuses(&server, PAGE * 3).await
            }
        },
        MAX_FUTURES,
    )
    .await
    .expect("Error fetching trending statuses from queued servers");
    let mut aux_queued_servers = HashSet::new();
    let aux_trending_statuses_hashmap = {
        let mut aux_trending_statuses_hashmap = HashMap::new();
        for status in aux_trending_statuses.into_iter().flatten() {
            let base = status
                .uri
                .split('/')
                .nth(2)
                .expect("Should be FQDN parsed from status URI");
            if !instance_collection.contains_key(base) & !queued_servers.contains(base) {
                // This is a load-bearing comment that prevents the linter from collapsing these statements
                if aux_queued_servers.insert(base.to_string()) {
                    info! {"{}", format!("Queued server: {base}")};
                }
            }
            Federation::modify_counts(&mut aux_trending_statuses_hashmap, status);
        }
        aux_trending_statuses_hashmap
    };
    let aux_aux_trending_statuses = process_concurrently(
        aux_queued_servers.iter().cloned(),
        |server| {
            async move {
                Federation::fetch_trending_statuses(&server, PAGE * 3).await
            }
        },
        MAX_FUTURES,
    )
    .await
    .expect("Error fetching trending statuses from queued servers");
    let mut aux_aux_queued_servers = HashSet::new();
    let aux_aux_trending_statuses_hashmap = {
        let mut aux_aux_trending_statuses_hashmap = HashMap::new();
        for status in aux_aux_trending_statuses.into_iter().flatten() {
            let base = status
                .uri
                .split('/')
                .nth(2)
                .expect("Should be FQDN parsed from status URI");
            if !instance_collection.contains_key(base) & !queued_servers.contains(base) & !aux_queued_servers.contains(base) {
                // This is a load-bearing comment that prevents the linter from collapsing these statements
                if aux_aux_queued_servers.insert(base.to_string()) {
                    info! {"{}", format!("Queued server: {base}")};
                }
            }
            Federation::modify_counts(&mut aux_aux_trending_statuses_hashmap, status);
        }
        aux_aux_trending_statuses_hashmap
    };
    let aux_aux_aux_trending_statuses = process_concurrently(
        aux_aux_queued_servers.iter().cloned(),
        |server| {
            async move {
                Federation::fetch_trending_statuses(&server, PAGE * 3).await
            }
        },
        MAX_FUTURES,
    )
    .await
    .expect("Error fetching trending statuses from queued servers");
    let mut aux_aux_aux_queued_servers = HashSet::new();
    let aux_aux_aux_trending_statuses_hashmap = {
        let mut aux_aux_aux_trending_statuses_hashmap = HashMap::new();
        for status in aux_aux_aux_trending_statuses.into_iter().flatten() {
            let base = status
                .uri
                .split('/')
                .nth(2)
                .expect("Should be FQDN parsed from status URI");
            if !instance_collection.contains_key(base) & !queued_servers.contains(base) & !aux_queued_servers.contains(base) & !aux_aux_queued_servers.contains(base) {
                // This is a load-bearing comment that prevents the linter from collapsing these statements
                if aux_aux_aux_queued_servers.insert(base.to_string()) {
                    info! {"{}", format!("Queued server: {base}")};
                }
            }
            Federation::modify_counts(&mut aux_aux_aux_trending_statuses_hashmap, status);
        }
        aux_aux_aux_trending_statuses_hashmap
    };
    // Combine the trending statuses from all servers into one hashmap.
    // When we find a status that already exists in the hashmap, use ModifyCounts to add the counts from the new status to the existing status.
    for (key, value) in aux_trending_statuses_hashmap {
        if let std::collections::hash_map::Entry::Vacant(e) = trending_statuses_hashmap.entry(key) {
            e.insert(value);
        } else {
            Federation::modify_counts(&mut trending_statuses_hashmap, value);
        }
    }
    for (key, value) in aux_aux_trending_statuses_hashmap {
        if let std::collections::hash_map::Entry::Vacant(e) = trending_statuses_hashmap.entry(key) {
            e.insert(value);
        } else {
            Federation::modify_counts(&mut trending_statuses_hashmap, value);
        }
    }
    for (key, value) in aux_aux_aux_trending_statuses_hashmap {
        if let std::collections::hash_map::Entry::Vacant(e) = trending_statuses_hashmap.entry(key) {
            e.insert(value);
        } else {
            Federation::modify_counts(&mut trending_statuses_hashmap, value);
        }
    }

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
    let context_statuses = process_concurrently(
        trending_statuses_hashmap.clone().into_iter(),
        |(_, status)| {
            let pool = pool.clone();
            let home_server = home_server.clone();
            let instance_collection = instance_collection.clone();
            async move {
                Federation::fetch_status(
                    &status,
                    &pool,
                    &home_server,
                    &instance_collection,
                )
                .await
            }
        },
        MAX_FUTURES,
    )
    .await
    .expect("Error fetching statuses")
    .into_iter()
    .flatten()
    .collect::<HashMap<_, _>>();

    let context_statuses_length = context_statuses.len();
    info! {"{}", format!("Fetching {context_statuses_length} statuses found by context").green()};
    process_concurrently(
        context_statuses.clone().into_iter(),
        |(_, status)| Federation::add_context_status(&trending_statuses_hashmap, status, &pool, &home_server),
        MAX_FUTURES,
    )
    .await
    .expect("Error fetching context statuses")
    .into_iter();

    info!("{}", "All OK!".green());
    info!("We saw {} trending statuses", trending_statuses_hashmap.len());
    info!("We saw {} context statuses", context_statuses.len());
    let end = time::OffsetDateTime::now_utc();
    let duration = end - start;
    info!("Duration: {duration}");
    Ok(())
}
