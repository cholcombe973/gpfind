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
use log::LogLevelFilter;
use multiqueue::{broadcast_queue, BroadcastReceiver, BroadcastSender};
use r2d2_gluster::*;
use r2d2::Pool;
use simplelog::{Config, TermLogger};

// Print all the files in the directories from the queu
fn print(recv: BroadcastReceiver<PathBuf>, cluster: Pool<GlusterPool>, workers: u64) {
    let mut handles = vec![];
    for i in 0..workers {
        trace!("Starting print worker: {}", i);
        let stream_consumer = recv.add_stream();
        let cluster = cluster.clone();
        handles.push(thread::spawn(move || {
            for p in stream_consumer {
                let sub_dir = GlusterDirectory {
                    dir_handle: cluster.get().unwrap()
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

fn search(queue: BroadcastSender<PathBuf>, p: PathBuf, cluster: Pool<GlusterPool>) {
    trace!("Starting directory search worker");
    thread::spawn(move || {
        let this = Path::new(".");
        let parent = Path::new("..");
        let sub_dir = GlusterDirectory {
            dir_handle: cluster.get().unwrap()
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
                        if let Err(e) = queue.try_send(pbuff.clone()){
                           error!("queue send failed: {}", e);
                        }
                        search(queue.clone(), pbuff, cluster.clone());
                    }
                }
                _ => {
                    //Nothing
                }
            }
        }
    });
}

fn list(
    server: &str,
    port: u16,
    volume: &str,
    path: &Path,
    workers: u64,
) -> Result<(), GlusterError> {
    let (send, recv) = broadcast_queue(workers);
    let cluster = GlusterPool::new(server, port, volume);
    let pool = Pool::new(cluster).unwrap();

    if let Err(e) = send.try_send(path.to_path_buf()){
        error!("Queueing path: {} failed: {}", path.display(), e);
    }
    let s = send.clone();
    search(s, path.to_path_buf(), pool.clone());

    for _ in 0..workers-1{
        let r = recv.clone();
        let p = pool.clone();
        thread::spawn(move || {
            print(r, p, workers-1);
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
    let workers = u64::from_str(matches.value_of("workers").unwrap()).unwrap();
    let port = u16::from_str(matches.value_of("port").unwrap()).unwrap();
    let server = matches.value_of("server").unwrap();
    let volume = matches.value_of("volume").unwrap();
    let path = Path::new(matches.value_of("path").unwrap());

    if let Err(e) = list(server, port, volume, path, workers) {
        println!("list directories failed: {}", e.to_string());
    }
}
