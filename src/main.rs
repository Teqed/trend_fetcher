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
use futures::stream::FuturesUnordered;
use futures::StreamExt;
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

#[tokio::main]
#[tracing::instrument]
/// The main function.
async fn main() -> Result<(), ()> {
    debug!("Starting");
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
    tasks = FuturesUnordered::new();
    tasks_remaining = queued_servers.len();
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
    let mut context_of_statuses = HashMap::new();
    let mut tasks_2 = FuturesUnordered::new();
    let mut tasks_remaining_2 = statuses.len();
    let statuses_vec: Vec<_> = statuses.iter().collect();
    let mut current_status_index = 0;
    while tasks_remaining_2 > 0 || !tasks_2.is_empty() {
        while tasks_2.len() < MAX_FUTURES && tasks_remaining_2 > 0 {
            tasks_remaining_2 -= 1;
            let (uri, status) = statuses_vec[current_status_index];
            tasks_2.push(Federation::fetch_status(
                uri,
                status,
                &pool,
                &home_server,
                &instance_collection,
            ));
            current_status_index += 1;
        }

        if let Some(context_of_status) = tasks_2.next().await {
            for (uri, status) in context_of_status {
                context_of_statuses.entry(uri).or_insert(status);
            }
        }
    }

    info! {"{}", format!("Fetching context statuses from {}", home_server.data.base)};
    let mut tasks_3 = FuturesUnordered::new();
    let context_of_statuses_length = context_of_statuses.len();
    let mut tasks_remaining_3 = context_of_statuses_length;
    let context_of_statuses_vec: Vec<_> = context_of_statuses.iter().collect();
    let mut current_context_of_status_index = 0;
    while tasks_remaining_3 > 0 || !tasks_3.is_empty() {
        while tasks_3.len() < MAX_FUTURES && tasks_remaining_3 > 0 {
            tasks_remaining_3 -= 1;
            let (_, status) = context_of_statuses_vec[current_context_of_status_index];
            tasks_3.push(Federation::add_context_status(
                &statuses,
                status.clone(),
                &pool,
                &home_server,
            ));
            current_context_of_status_index += 1;
        }

        if (tasks_3.next().await).is_some() {
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
