use std::env;
use std::env::VarError;
use std::fs::read_to_string;
use std::path::Path;
use serde::Deserialize;
use kafka::KafkaConfiguration;

pub(crate) mod kafka;


#[derive(Deserialize, Debug)]
pub struct ConfigurationProperties {
    pub(crate) kafka: KafkaConfiguration,
}

impl Default for ConfigurationProperties {
    fn default() -> Self {
        Self::read_config()
    }
}

impl ConfigurationProperties {
    // TODO: split this out.

    pub(crate) fn read_config() -> ConfigurationProperties {
        let config_file = env::var("CONFIG_PROPS")
            .or(Ok::<String, VarError>("resources/config.toml".to_string()))
            .unwrap();
        let config = read_to_string(Path::new(&config_file))
            .map(|toml| toml::from_str::<ConfigurationProperties>(toml.as_str()).ok())
            .or_else(|e| {
                println!("Error reading configuration properties: {:?}.", e);
                Ok::<Option<ConfigurationProperties>, toml::de::Error>(Some(ConfigurationProperties::default()))
            })
            .ok()
            .flatten()
            .unwrap();
        config
    }
}
