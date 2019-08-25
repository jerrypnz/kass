use std::convert::Into;
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
use cdrs::types::value::Value;
use cdrs::types::CBytes;
use futures::executor::{block_on, ThreadPoolBuilder};
use serde_json::Map;

use crate::errors::AppResult;
use crate::future_utils::{self, SpawnFuture};
use crate::params;
use crate::types::decode_value;

pub type CurrentSession = Session<RoundRobinSync<TcpConnectionPool<NoneAuthenticator>>>;

fn row_to_json(meta: &RowsMetadata, row: &Vec<CBytes>) -> AppResult<String> {
    let mut i = 0;
    let mut obj = Map::with_capacity(meta.columns_count as usize);

    for col in &meta.col_specs {
        let name = col.name.as_plain();
        let value = decode_value(col, &row[i])?;
        obj.insert(name, serde_json::to_value(value)?);
        i = i + 1;
    }

    serde_json::to_string(&obj).map_err(|e| e.into())
}

fn process_response(resp: &Frame) -> AppResult<()> {
    let body = resp.get_body()?;

    if let ResponseBody::Result(ResResultBody::Rows(rows)) = body {
        let meta = rows.metadata;
        for row in rows.rows_content {
            match row_to_json(&meta, &row) {
                Ok(json) => println!("{}", json),
                Err(err) => eprintln!("{}", err),
            }
        }
    }
    Ok(())
}

fn query_prepared(
    session: &CurrentSession,
    query: &PreparedQuery,
    vals: Vec<Value>,
) -> AppResult<()> {
    let query_vals = QueryValues::SimpleValues(vals);
    let params = QueryParamsBuilder::new().values(query_vals).finalize();
    let resp = session.exec_with_params(query, params)?;
    process_response(&resp)
}

pub fn query_with_args(session: CurrentSession, cql: &str, args: Vec<&str>) -> AppResult<()> {
    let prepared = session.prepare(cql)?;
    let vals = params::parse_args(args)?;
    let session = Arc::new(session);

    //TODO configurable parallelism
    let mut pool = ThreadPoolBuilder::new()
        .pool_size(5)
        .create()
        .expect("Failed to create thread pool");

    let fut = future_utils::traverse(vals, |vs| {
        let sess = session.clone();
        let q = prepared.clone();
        pool.spawn_future(move || query_prepared(&sess, &q, vs))
    });

    block_on(fut)?;

    Ok(())
}

pub fn query(session: &CurrentSession, cql: &str) -> AppResult<()> {
    let resp = session.query(cql)?;
    process_response(&resp)
}

pub fn connect(host: &str) -> AppResult<CurrentSession> {
    let node = NodeTcpConfigBuilder::new(host, NoneAuthenticator {})
        .connection_timeout(Duration::from_secs(10)) //TODO CLI option for timeout
        .build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let session = new_session(&cluster_config, RoundRobinSync::new())?;
    Ok(session)
}
