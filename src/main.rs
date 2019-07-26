extern crate cdrs;
extern crate clap;
extern crate itertools;
extern crate serde_json;

mod errors;
mod json;
mod params;

use errors::{AppResult, AppError};

use clap::{App, Arg};

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::frame::Frame;
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;
use std::time::Duration;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;

fn connect(host: &str) -> AppResult<CurrentSession> {
    let node = NodeTcpConfigBuilder::new(host, NoneAuthenticator {})
        .connection_timeout(Duration::from_secs(10)) //TODO CLI option for timeout
        .build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let session = new_session(&cluster_config, RoundRobin::new())?;
    Ok(session)
}

fn process_response(resp: &Frame) -> AppResult<()> {
    let body = resp.get_body()?;

    let meta = body.as_rows_metadata().ok_or(AppError::general("row metadata not found in response"))?;
    let rows = body.into_rows();
    if let Some(rows) = rows {
        for row in rows {
            match json::row_to_json(&meta, &row) {
                Ok(json) => println!("{}", json),
                Err(err) => eprintln!("{}", err),
            }
        }
    }
    Ok(())
}

fn query_with_args(session: &CurrentSession, cql: &str, args: Vec<&str>) -> AppResult<()> {
    let prepared = session.prepare(cql)?;
    let vals = params::parse_args(args)?;

    for val in vals {
        let query_vals = QueryValues::SimpleValues(val);
        let params = QueryParamsBuilder::new().values(query_vals).finalize();
        let resp = session.exec_with_params(&prepared, params)?;
        process_response(&resp)?;
    }

    Ok(())
}

fn query(session: &CurrentSession, cql: &str) -> AppResult<()> {
    let resp = session.query(cql)?;
    process_response(&resp)
}

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

    let result = connect(host).and_then(|session| match params {
        Some(args) => query_with_args(&session, cql, args),
        None => query(&session, cql),
    });

    if let Err(err) = result {
        eprintln!("{}", err);
    }
}

#[cfg(test)]
#[macro_use]
extern crate matches;
