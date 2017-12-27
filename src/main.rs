#[macro_use]
extern crate clap;
extern crate gfapi_sys;
extern crate libc;
#[macro_use]
extern crate log;
extern crate multiqueue;
extern crate r2d2;
extern crate r2d2_gluster;
extern crate simplelog;

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::thread;

use clap::{App, Arg};
use gfapi_sys::gluster::*;
use libc::{DT_DIR, DT_REG};
use multiqueue::{broadcast_queue, BroadcastReceiver, BroadcastSender};
use simplelog::{Config, TermLogger};

// Print all the files in the directories from the queue
// This is limited to workers -1 number of print threads
fn print(recv: BroadcastReceiver<PathBuf>, s: String, port: u16, v: String, workers: u64) {
    let mut handles = vec![];
    for i in 0..workers {
        trace!("Starting print worker: {}", i);
        let stream_consumer = recv.add_stream();
        handles.push(thread::spawn(move || {
            debug!("Connecting to gluster: {}:{}/{}", s, port, v);
            let conn = Gluster::connect(&v, &s, port).expect("Connection to gluster failed");
            for p in stream_consumer {
                let sub_dir = GlusterDirectory {
                    dir_handle: conn
                        .opendir(&p)
                        .expect(&format!("failed to open dir: {}", p.display())),
                };
                for s in sub_dir {
                    match s.file_type {
                        DT_REG => {
                            println!("{}/{}", p.display(), s.path.display());
                        }
                        _ => {
                            //Nothing
                        }
                    }
                }
            }
        }));
    }
    // Take notice that I drop the reader - this removes it from
    // the queue, meaning that the readers in the new threads
    // won't get starved by the lack of progress from recv
    recv.unsubscribe();

    for t in handles {
        t.join().expect("Couldn't join on the associated thread");
    }
}

// This will end up starting 1 thread per directory recursion
// TODO: This might be a bit much.  Might need to back this off
// to just 1 or a few threads total
fn search(queue: BroadcastSender<PathBuf>, p: PathBuf, server: String, port: u16, v: String) {
    trace!("Starting directory search worker");
    thread::spawn(move || {
        debug!("Connecting to gluster: {}:{}/{}", server, port, v);
        let conn = Gluster::connect(&v, &server, port).expect("Connection to gluster failed");
        let this = Path::new(".");
        let parent = Path::new("..");
        let sub_dir = GlusterDirectory {
            dir_handle: conn
                .opendir(&p)
                .expect(&format!("failed to open dir: {}", p.display())),
        };
        for s in sub_dir {
            match s.file_type {
                DT_DIR => {
                    if !(s.path == this || s.path == parent) {
                        // Push the dir onto the queue for another thread to handle
                        let mut pbuff = p.clone();
                        pbuff.push(s.path);
                        if let Err(e) = queue.try_send(pbuff.clone()) {
                            error!("queue send failed: {}", e);
                        }
                        trace!("Recurse");
                        search(queue.clone(), pbuff, server.clone(), port, v.clone());
                    }
                }
                _ => {
                    //Nothing
                }
            }
        }
    });
}

fn list(server: &str, port: u16, volume: &str, path: &Path, workers: u64) -> Result<(), String> {
    debug!("Starting broadcast queue");
    let (send, recv) = broadcast_queue(workers);
    debug!("Seeding first path to check");
    if let Err(e) = send.try_send(path.to_path_buf()) {
        error!("Queueing path: {} failed: {}", path.display(), e);
    }
    let s = send.clone();
    debug!("Beginning search");
    search(s, path.to_path_buf(), server.to_string(), port.clone(), volume.to_string());

    for _ in 0..workers - 1 {
        let r = recv.clone();
        thread::spawn(move || {
            print(r, server.to_string(), port.clone(), volume.to_string(), workers - 1);
        });

        // What condition can I use to stop?
    }
    drop(send);

    Ok(())
}

fn main() {
    let matches = App::new("gpfind")
        .version(crate_version!())
        .author(crate_authors!())
        .about("Lists directories")
        .arg(
            Arg::with_name("path")
                .default_value("/")
                .long("path")
                .help("The gluster path to list")
                .short("p")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .default_value("24007")
                .long("port")
                .help("The gluster port to connect to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("server")
                .default_value("localhost")
                .long("server")
                .help("The gluster server to connect to")
                .short("s")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("volume")
                .long("volume")
                .help("The gluster volume to connect to")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("workers")
                .default_value("5")
                .long("workers")
                .help("The number of workers to use in the threadpool")
                .short("w")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("v")
                .short("v")
                .multiple(true)
                .help("Sets the level of verbosity"),
        )
        .get_matches();

    let level = match matches.occurrences_of("v") {
        0 => simplelog::LogLevelFilter::Info, //default
        1 => simplelog::LogLevelFilter::Debug,
        _ => simplelog::LogLevelFilter::Trace,
    };

    TermLogger::init(level, Config::default()).unwrap();
    let workers = u64::from_str(matches.value_of("workers").unwrap()).unwrap();
    let port = u16::from_str(matches.value_of("port").unwrap()).unwrap();
    let server = matches.value_of("server").unwrap();
    let volume = matches.value_of("volume").unwrap();
    let path = Path::new(matches.value_of("path").unwrap());

    if let Err(e) = list(server, port, volume, path, workers) {
        println!("list directories failed: {}", e.to_string());
    }
}
