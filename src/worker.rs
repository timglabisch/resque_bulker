use redis::Connection;
use connection::ConnectionFactory;
use redis::PipelineCommands;
use std::time::{SystemTime, Duration};
use redis::RedisError;
use std::thread;

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


            let xxx = buffer
                .iter()
                .map(|d : &DequeueResult| {
                    d.raw.clone()
                })
                .collect::<Vec<String>>()
                .join(",")
            ;



            println!("yay, we've {} messages", buffer.len());

        }

        println!("finish");

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