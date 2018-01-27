use redis::Connection;
use connection::ConnectionFactory;
use redis::PipelineCommands;
use std::time::{SystemTime, Duration};
use redis::RedisError;
use std::thread;
use std::fmt::Write;

pub struct DequeueResult {
    queue: String,
    raw: String
}

impl DequeueResult {
    pub fn new(queue: String, raw: String) -> Self {
        DequeueResult {
            queue,
            raw
        }
    }
}

pub struct Worker {
    connection_factory: ConnectionFactory,
}

impl Worker {

    pub fn new() -> Self {
        Worker {
            connection_factory: ConnectionFactory::new()
        }
    }

    pub fn run(&mut self) {

        // buffer muss hier oben deklariert sein,
        // falls die verbindung zum redis mal verloren geht.
        let mut buffer : Vec<DequeueResult> = vec![];

        'main_dequeue_loop: loop {

            let connection_try = self.connection_factory.get_new_redis_connection();

            let connection: Connection = match connection_try {
                Err(e) => {

                    if ::STOP_PROG.load(::std::sync::atomic::Ordering::Relaxed) {

                        if buffer.len() == 0 {
                            // this is safe, we dont have something in our queue
                            break 'main_dequeue_loop;
                        }

                        println!("now we're in trouble, the program should exit, but re couldn't store our msgs, we're going to sleep and retry");
                    }

                    println!("sleep, and retry to get connection.");
                    thread::sleep_ms(500);
                    continue;
                },
                Ok(c) => c
            };

            loop {

                // could happen in an error case.
                if buffer.len() >= 500 {
                    break;
                }

                let msg : DequeueResult = match self.dequeue(&connection) {
                    Ok(Some(res)) => {
                        res
                    },
                    Ok(None) => {
                        println!("nothing more to dequeue");
                        break;
                    },
                    Err(e) => {
                        println!("error, could not dequeue");
                        break 'main_dequeue_loop;
                    }
                };

                // now we've the msg...
                buffer.push(msg);

                if buffer.len() >= 500 {
                    break;
                }

                if ::STOP_PROG.load(::std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
            }

            if ::STOP_PROG.load(::std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            if buffer.len() == 0 {
                println!("nothing to do, we're going on.");
                continue;
            }

            println!("try to reenqueue {} messages", buffer.len());

            match self.reenqueue(&connection, &buffer) {
                Ok(_) => {
                    let mut buffer = vec![];
                },
                Err(e) => {
                    println!("there was an error when reenqueuing");
                    continue;
                }
            };


            println!("successfully reenqueued {} messages", buffer.len());

        }

        println!("worker finish");

    }

    pub fn reenqueue(&self, con : &Connection, data : &Vec<DequeueResult>) -> Result<(), Box<::std::error::Error>> {

        let data_buffer = data
            .iter()
            .map(|d : &DequeueResult| {
                d.raw.clone()
            })
            .collect::<Vec<String>>()
            .join(",")
        ;

        let mut buffer = String::from("{\"class\":\"FooJob\",\"args\":[");
        buffer.write_str(&data_buffer)?;
        buffer.write_str("]}")?;

        let enqueue: () = ::redis::cmd("rpush")
            .arg("resque:queue:default:foo")
            .arg(&buffer)
            .query(con)?;

        Ok(())
    }

    pub fn dequeue(&self, con : &Connection) -> Result<Option<DequeueResult>, RedisError>
    {
        let res: Vec<(String, String)> = ::redis::cmd("blpop")
            .arg("resque:queue:default")
            .arg("resque:queue:default1")
            .arg(1)
            .query(con)?;

        match res.first() {
            None => Ok(None),
            Some(&(ref queue, ref raw)) => {
                Ok(Some(DequeueResult::new(queue.clone(), raw.clone())))
            }
        }
    }

}