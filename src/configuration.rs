use serde::Deserialize;

#[derive(Deserialize)]
/// Configuration struct for the application.
pub struct Config {
    /// Database configuration.
    pub database: Database,
    /// Servers configuration.
    pub servers: Servers,
}

#[derive(Deserialize)]
/// Servers configuration.
pub struct Servers {
    /// Home server.
    pub home: String,
    /// Authenticated servers.
    pub authenticated: Vec<String>,
    /// Unauthenticated servers.
    pub unauthenticated: Vec<String>,
}

#[derive(Deserialize)]
/// Database configuration.
pub struct Database {
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

/// Loads the configuration from the "config.toml" file.
pub fn load_config() -> Config {
    let config_string = match std::fs::read_to_string("config.toml") {
        Ok(config_string) => config_string,
        Err(error) => {panic!("config.toml should exist, instead we saw an error: {error}")}
    };
    let config: Config = match toml::from_str(&config_string) {
        Ok(config) => config,
        Err(error) => {panic!("config.toml should be valid TOML, instead we saw an error: {error}")}
    };
    config
}
