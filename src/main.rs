#![macro_use]
extern crate cdrs;
extern crate clap;
extern crate futures;
extern crate itertools;
extern crate serde;
extern crate serde_json;
extern crate chrono;
extern crate uuid;

mod cass;
mod errors;
mod future_utils;
mod params;
mod types;

use clap::{App, Arg};

fn main() {
    let matches = App::new("CQL")
        .version("0.1.0")
        .about("Command line Cassandra CQL client")
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .value_name("HOST:PORT")
                .help("The Cassandra host to connect to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("QUERY")
                .help("The query to run")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("PARAM")
                .short("p")
                .long("param")
                .takes_value(true)
                .multiple(true)
                .value_name("M-N")
                .help("A query parameter"),
        )
        .get_matches();

    let host = matches.value_of("host").unwrap_or("127.0.0.1:19142");
    let cql = matches.value_of("QUERY").expect("QUERY is required");
    let params: Option<Vec<&str>> = matches.values_of("PARAM").map(|x| x.collect());

    let result = cass::connect(host).and_then(move |session| match params {
        Some(args) => cass::query_with_args(session, cql, args),
        None => cass::query(&session, cql),
    });

    if let Err(err) = result {
        eprintln!("{}", err);
    }
}
