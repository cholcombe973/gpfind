#[macro_use]
extern crate clap;
extern crate crossbeam;
extern crate gfapi_sys;
extern crate libc;
#[macro_use]
extern crate log;
extern crate scoped_threadpool;
extern crate simplelog;

use std::path::Path;
use std::str::FromStr;

use clap::{App, Arg};
use crossbeam::sync::MsQueue;
use gfapi_sys::gluster::*;
use libc::{DT_DIR, DT_REG};
use log::LogLevelFilter;
use scoped_threadpool::Pool;
use simplelog::{Config, TermLogger};

fn list(
    server: &str,
    port: u16,
    volume: &str,
    path: &Path,
    workers: u32,
) -> Result<(), GlusterError> {
    println!("Connecting to: {}", server);
    let cluster = Gluster::connect(volume, server, port)?;

    //Helpers to avoid adding self and parent paths
    let this = Path::new(".");
    let parent = Path::new("..");

    let mut queue = MsQueue::new();
    // Seed the queue with the first directory path
    queue.push(path.to_path_buf());
    let mut done = false;

    let mut worker_pool = Pool::new(workers);

    while !done {
        worker_pool.scoped(|scoped| {
            // Push directories onto the queue
            scoped.execute(|| {
                let p = match queue.try_pop() {
                    Some(p) => p,
                    None => {
                        //println!("Queue empty");
                        done = true;
                        return;
                    }
                };
                //println!("try_pop: {}", p.display());
                let sub_dir = GlusterDirectory { dir_handle: cluster.opendir(&p).unwrap() };
                for s in sub_dir {
                    match s.file_type {
                        DT_REG => {
                            println!("{}/{}", p.display(), s.path.display());
                        }
                        DT_DIR => {
                            if !(s.path == this || s.path == parent) {
                                // Push the dir onto the queue for another thread to handle
                                let mut pbuff = p.clone();
                                pbuff.push(s.path);
                                //println!("push dir: {}", pbuff.display());
                                queue.push(pbuff);
                            }
                        }
                        _ => {
                            //Nothing
                        }
                    }
                }
            });
        });

        // What condition can I use to stop?
    }
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
                .short("v")
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
        .get_matches();

    TermLogger::new(LogLevelFilter::Debug, Config::default()).unwrap();
    let workers = u32::from_str(matches.value_of("workers").unwrap()).unwrap();
    let port = u16::from_str(matches.value_of("port").unwrap()).unwrap();
    let server = matches.value_of("server").unwrap();
    let volume = matches.value_of("volume").unwrap();
    let path = Path::new(matches.value_of("path").unwrap());

    if let Err(e) = list(server, port, volume, path, workers) {
        println!("list directories failed: {}", e.to_string());
    }
}
