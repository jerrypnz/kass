extern crate cdrs;
extern crate clap;

use clap::{App, Arg};

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;
use cdrs::types::rows::Row;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;

fn connect(host: &str) -> CurrentSession {
    let node = NodeTcpConfigBuilder::new(host, NoneAuthenticator {}).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let session = new_session(&cluster_config, RoundRobin::new())
        .expect(format!("Failed to connect to {}", host).as_str());
    session
}

fn query(session: &CurrentSession, cql: &str) {
    let rows = session
        .query(cql)
        .expect(format!("Failed to execute query: {}", cql).as_str())
        .get_body()
        .expect(format!("Failed to get query result: {}", cql).as_str())
        .into_rows()
        .expect(format!("Failed to get query result: {}", cql).as_str());

    // TODO Decode rows into a struct, json/csv formatting etc.
    println!("Query returned {} rows", rows.len());
}

fn main() {
    let matches = App::new("CQL")
        .version("0.1.0")
        .author("Jerry Peng <pr2jerry@gmail.com>")
        .about("Command line Cassandra CQL client")
        .arg(Arg::with_name("host")
             .short("h")
             .long("host")
             .value_name("HOST:PORT")
             .help("The Cassandra host to connect to")
             .takes_value(true))
        .arg(Arg::with_name("QUERY")
             .help("The query to run")
             .required(true)
             .index(1))
        .get_matches();

    let host = matches.value_of("host").unwrap_or("127.0.0.1:19142");
    let cql = matches.value_of("QUERY").expect("QUERY is required");

    let session = connect(host);
    query(&session, cql);
}
