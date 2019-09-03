use std::sync::Arc;
use std::time::Duration;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::frame::frame_response::ResponseBody;
use cdrs::frame::frame_result::ResResultBody;
use cdrs::frame::frame_result::RowsMetadata;
use cdrs::frame::Frame;
use cdrs::load_balancing::RoundRobinSync;
use cdrs::query::*;
use cdrs::types::CBytes;
use clap::ArgMatches;
use colored_json::ColoredFormatter;
use futures::executor::{block_on, ThreadPoolBuilder};
use serde_json::ser::CompactFormatter;
use serde_json::{Map, Value as JsonValue};

use crate::errors::AppResult;
use crate::future_utils::{self, SpawnFuture};
use crate::params;
use crate::types::ColValue;

pub type CurrentSession = Session<RoundRobinSync<TcpConnectionPool<NoneAuthenticator>>>;

enum ColorOpt {
    Auto,
    Never,
    Always,
}

pub struct Config {
    host: String,
    color: ColorOpt,
    parallelism: usize,
    pretty: bool,
}

impl Config {
    pub fn from_matches(matches: &ArgMatches) -> AppResult<Self> {
        let mut host = matches
            .value_of("host")
            .unwrap_or("localhost:9042")
            .to_string();

        if host.find(':').is_none() {
            host.push_str(":9042");
        }

        let color = match matches.value_of("color") {
            Some("never") => ColorOpt::Never,
            Some("always") => ColorOpt::Always,
            _ => ColorOpt::Auto,
        };
        let parallelism = match matches.value_of("parallelism") {
            Some(x) => x.parse().unwrap_or(5),
            None => 5,
        };
        let pretty = matches.is_present("pretty");

        Ok(Self {
            host,
            color,
            parallelism,
            pretty,
        })
    }
}

pub fn run_query(
    config: Config,
    query: &str,
    params: Option<Vec<params::Values>>,
) -> AppResult<()> {
    let session = connect(config.host.as_str())?;
    match params {
        Some(params) => parallel_query(session, query, params, config),
        None => simple_query(&session, query, &config),
    }
}

fn connect(host: &str) -> AppResult<CurrentSession> {
    let node = NodeTcpConfigBuilder::new(host, NoneAuthenticator {})
        .connection_timeout(Duration::from_secs(10)) //TODO CLI option for timeout
        .build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let session = new_session(&cluster_config, RoundRobinSync::new())?;
    Ok(session)
}

fn prepared_query(
    session: &CurrentSession,
    query: &PreparedQuery,
    vals: params::Values,
    config: &Config,
) -> AppResult<()> {
    let query_vals = QueryValues::SimpleValues(vals);
    let params = QueryParamsBuilder::new().values(query_vals).finalize();
    let resp = session.exec_with_params(query, params)?;
    write_results(&resp, config)
}

fn parallel_query(
    session: CurrentSession,
    cql: &str,
    vals: Vec<params::Values>,
    config: Config,
) -> AppResult<()> {
    let prepared = session.prepare(cql)?;
    let session = Arc::new(session);
    let config = Arc::new(config);

    let mut pool = ThreadPoolBuilder::new()
        .pool_size(config.parallelism)
        .create()
        .expect("Failed to create thread pool");

    let fut = future_utils::traverse(vals, |vs| {
        let sess = session.clone();
        let q = prepared.clone();
        let conf = config.clone();
        pool.spawn_future(move || prepared_query(&sess, &q, vs, &conf))
    });

    block_on(fut)?;

    Ok(())
}

fn simple_query(session: &CurrentSession, cql: &str, config: &Config) -> AppResult<()> {
    let resp = session.query(cql)?;
    write_results(&resp, config)
}

fn write_results(resp: &Frame, config: &Config) -> AppResult<()> {
    let body = resp.get_body()?;

    if let ResponseBody::Result(ResResultBody::Rows(rows)) = body {
        let meta = rows.metadata;
        for row in rows.rows_content {
            write_row(&meta, &row, config)
        }
    }
    Ok(())
}

fn write_row(meta: &RowsMetadata, row: &Vec<CBytes>, config: &Config) {
    let fmt = ColoredFormatter::new(CompactFormatter {});
    let result = row_to_json(meta, row)
        .and_then(|x| fmt.to_colored_json_auto(&x).map_err(|x| x.into()));

    match result {
        Ok(json) => println!("{}", json),
        // TODO Better error reporting
        Err(err) => eprintln!("{}", err),
    }
}

fn row_to_json(meta: &RowsMetadata, row: &Vec<CBytes>) -> AppResult<JsonValue> {
    let mut i = 0;
    let mut obj = Map::with_capacity(meta.columns_count as usize);

    for col in &meta.col_specs {
        let name = col.name.as_plain();
        let value = ColValue::decode(&col.col_type, &row[i])?;
        obj.insert(name, serde_json::to_value(value)?);
        i = i + 1;
    }
    Ok(JsonValue::Object(obj))
}