use serde::Deserialize;
use std::fs;
use std::net::IpAddr;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(deserialize_with = "deserialize_ip_addr")]
    pub host: IpAddr,
    pub port: u16,
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
}

fn deserialize_ip_addr<'de, D>(deserializer: D) -> Result<IpAddr, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    s.parse().map_err(serde::de::Error::custom)
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&contents)?;
        Ok(config)
    }
} 