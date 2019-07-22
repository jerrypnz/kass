extern crate cdrs;
extern crate clap;

mod json;

use clap::{App, Arg};

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::error::Result;
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;
use serde_json;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;

fn connect(host: &str) -> CurrentSession {
    let node = NodeTcpConfigBuilder::new(host, NoneAuthenticator {}).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let session = new_session(&cluster_config, RoundRobin::new())
        .expect(format!("Failed to connect to {}", host).as_str());
    session
}

fn query(session: &CurrentSession, cql: &str) -> Result<()> {
    let body = session.query(cql).and_then(|x| x.get_body())?;

    let meta = body.as_rows_metadata();
    let rows = body.into_rows();

    match rows {
        Some(rows) => {
            let meta = meta.unwrap();
            // println!(
            //     "Query returned {} rows, {} columns",
            //     rows.len(),
            //     meta.columns_count
            // ) ;
            for row in rows {
                let json = json::row_to_json(&meta, &row)?;
                let json_str = serde_json::to_string(&json).expect("failed to print json");
                println!("{}", json_str);
            }
        }
        None => println!("Query didn't return a result"),
    };

    Ok(())
}

fn main() -> Result<()> {
    let matches = App::new("CQL")
        .version("0.1.0")
        .author("Jerry Peng <pr2jerry@gmail.com>")
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
        .get_matches();

    let host = matches.value_of("host").unwrap_or("127.0.0.1:19142");
    let cql = matches.value_of("QUERY").expect("QUERY is required");

    let session = connect(host);
    query(&session, cql)
}
