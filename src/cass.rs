use crate::errors::{AppError, AppResult};
use crate::future_utils::{self, SpawnFuture};
use crate::json;
use crate::params;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::frame::Frame;
use cdrs::load_balancing::RoundRobinSync;
use cdrs::query::*;
use cdrs::types::value::Value;
use futures::executor::{block_on, ThreadPoolBuilder};
use std::sync::Arc;
use std::time::Duration;

pub type CurrentSession = Session<RoundRobinSync<TcpConnectionPool<NoneAuthenticator>>>;

fn process_response(resp: &Frame) -> AppResult<()> {
    let body = resp.get_body()?;

    let meta = body
        .as_rows_metadata()
        .ok_or(AppError::general("row metadata not found in response"))?;
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
