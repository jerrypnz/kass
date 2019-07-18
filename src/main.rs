extern crate cdrs;
extern crate clap;

use std::fmt::Display;

use clap::{App, Arg};

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;
use cdrs::types::rows::{Row};
use cdrs::types::{IntoRustByIndex};
use cdrs::error::Result;
use cdrs::frame::frame_result::{RowsMetadata, ColType};

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;

fn connect(host: &str) -> CurrentSession {
    let node = NodeTcpConfigBuilder::new(host, NoneAuthenticator {}).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let session = new_session(&cluster_config, RoundRobin::new())
        .expect(format!("Failed to connect to {}", host).as_str());
    session
}

fn print_val<R: Display, T: IntoRustByIndex<R>>(i: usize, row: &T) {
    let value = row.get_by_index(i).expect("Failed to get value");
    match value {
        Some(value) => print!("{}\t", value),
        None => print!("\t"),
    };
}

fn print_row(meta: &RowsMetadata, row: &Row) {
    let mut i = 0;
    for col in &meta.col_specs {
        match &col.col_type.id {
            ColType::Int => print_val::<i32, Row>(i, row),
            ColType::Varchar => print_val::<String, Row>(i, row),
            ColType::Null => print!("null\t"),
            _ => print!("\t"),
        };
        i = i+1;
    }
    println!("");
}

fn query(session: &CurrentSession, cql: &str) -> Result<()> {
    let body = session
        .query(cql)
        .and_then(|x| x.get_body())?;

    let meta = body.as_rows_metadata();
    let rows = body.into_rows();

    match rows {
        Some(rows) => {
            let meta = meta.unwrap();
            println!("Query returned {} rows, {} columns", rows.len(), meta.columns_count);
            for row in rows {
                print_row(&meta, &row);
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
    query(&session, cql)
}
