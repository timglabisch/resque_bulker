use config::config_raw::RawConfig;

#[derive(Debug, Deserialize)]
pub struct QueueConfig {
    read_queue: String,
    write_queue: String,
    write_class: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    queue_configs: Vec<QueueConfig>
}

impl Config {

    pub fn from_raw_config(raw_config : RawConfig) -> Result<Config, String> {

        let queue_configs = raw_config.queue_configs
            .into_iter()
            .map(|raw_queue|{
                Ok(QueueConfig {
                    read_queue: raw_queue.read_queue.ok_or_else(|| "configuration read_queue is required".to_string())?,
                    write_queue: raw_queue.write_queue.ok_or_else(|| "configuration write_queue is required".to_string())?,
                    write_class: raw_queue.write_class.ok_or_else(|| "configuration write_class is required".to_string())?
                })
            })
            .collect::<Result<Vec<QueueConfig>, String>>()?;

        Ok(Config {
            queue_configs: queue_configs
        })
    }


}