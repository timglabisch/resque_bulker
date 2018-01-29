#[macro_use]
extern crate chan;
extern crate chan_signal;
extern crate redis;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate lazy_static;
extern crate toml;

use redis::{Commands, PipelineCommands, transaction};
use redis::RedisError;
use std::thread;
use std::time::Duration;
use chan_signal::Signal;
mod worker;
mod connection;
use std::sync::atomic::AtomicBool;
use worker::Worker;
mod config;

lazy_static! {

    static ref STOP_PROG : AtomicBool = AtomicBool::new(false);

}


fn main() {

    // Signal gets a value when the OS sent a INT or TERM signal.
    let signal = chan_signal::notify(&[Signal::INT, Signal::TERM]);
    // When our work is complete, send a sentinel value on `sdone`.
    let (sdone, rdone) = chan::sync(0);
    // Run work.



    thread::spawn(move || do_main(sdone));

    loop {
        chan_select! {
            signal.recv() -> signal => {
                println!("received signal: {:?}", signal);
                STOP_PROG.store(true, ::std::sync::atomic::Ordering::Relaxed);
            },
            rdone.recv() => {
                println!("Program completed normally.");
                return;
            }
        }
    }
}


fn do_main(_sdone: chan::Sender<()>) {
    let mut worker = Worker::new();

    worker.run();
}