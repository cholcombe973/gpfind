#[macro_use]
extern crate clap;
extern crate crossbeam;
extern crate gfapi_sys;
extern crate libc;

use std::path::Path;
use std::str::FromStr;

use clap::{App, Arg};
use crossbeam::sync::MsQueue;
use gfapi_sys::gluster::*;
use libc::{DT_DIR, DT_REG};

fn list(server: &str, port: u16, volume: &str, path: &Path) -> Result<(), GlusterError>{
    let cluster = Gluster::connect(volume, server, port)?;
    let d = GlusterDirectory { dir_handle: cluster.opendir(path)? };

    let mut queue = MsQueue::new();
    let mut done = false;
    let this = Path::new(".");
    let parent = Path::new("..");

        println!("{}", dir_entry.path.display());
        while !done {
            crossbeam::scope(|scope| {
                scope.spawn(|| {
                    for dir_entry in d {
                        let p = queue.pop();
                        let sub_dir = GlusterDirectory { dir_handle: cluster.opendir(&p)? };
                        for s in sub_dir{
                        match s.file_type {
                            DT_REG => {
                                println!("{}", s.path.display());
                            }
                            DT_DIR => {
                                queue.push(sub_dir.path);
                            }
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
                .help("The gluster port to connect to")
                .required(true)
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
                .long("server")
                .help("The gluster server to connect to")
                .required(true)
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
        .get_matches();
    let port = u16::from_str(matches.value_of("port").unwrap()).unwrap();
    let server = matches.value_of("server").unwrap();
    let volume = matches.value_of("volume").unwrap();
    let path = Path::new(matches.value_of("path").unwrap());

    list(server, port, volume, path);
}
