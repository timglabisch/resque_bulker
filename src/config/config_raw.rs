use std::fs::File;
use std::io::Read;
use ::toml;

#[derive(Debug, Deserialize)]
pub struct RawQueueConfig {
    pub read_queue: Option<String>,
    pub write_queue: Option<String>,
    pub write_class: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RawConfig {
    pub queue_configs: Vec<RawQueueConfig>
}

impl RawConfig {

    pub fn new() -> Result<RawConfig, Box<::std::error::Error>> {

        let mut buffer = String::new();
        let mut file = File::open("queue_conf.toml").map_err(|_|"could not read queue_conf.toml".to_string())?;

        file.read_to_string(&mut buffer)?;


        let config : RawConfig = toml::from_str(&buffer)?;

        Ok(config)
    }

}