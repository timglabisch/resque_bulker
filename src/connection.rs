use redis;
use redis::RedisError;
use redis::Connection;

pub struct ConnectionFactory;

impl ConnectionFactory {

    pub fn new() -> Self {
        ConnectionFactory {}
    }

    pub fn get_new_redis_connection(&self) -> Result<Connection, RedisError>
    {
        let client = try!(redis::Client::open("redis://127.0.0.1/"));
        let con = try!(client.get_connection());

        Ok(con)
    }

}