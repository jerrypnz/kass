extern crate bigdecimal;
extern crate cdrs;
extern crate chrono;
extern crate clap;
extern crate futures;
extern crate itertools;
extern crate serde;
extern crate serde_json;
extern crate uuid;

mod core;
mod errors;
mod future_utils;
mod params;
mod types;

use self::clap::{App, AppSettings, Arg};
use self::errors::{AppError, AppResult};

fn app() -> App<'static, 'static> {
    App::new("CQL")
        .version("0.1.0")
        .about("Command line Cassandra CQL client")
        .setting(AppSettings::TrailingVarArg)
        .setting(AppSettings::UnifiedHelpMessage)
        .setting(AppSettings::ColoredHelp)
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .takes_value(true)
                .value_name("HOST:PORT")
                .help("The Cassandra host to connect to"),
        )
        .arg(
            Arg::with_name("color")
                .short("C")
                .long("color")
                .takes_value(true)
                .possible_values(&["auto", "always", "never"])
                .default_value("auto")
                .help("When to use terminal colors"),
        )
        .arg(
            Arg::with_name("pretty")
                .long("pretty")
                .help("Pretty print JSON"),
        )
        .arg(
            Arg::with_name("parallelism")
                .short("P")
                .long("parallelism")
                .takes_value(true)
                .default_value("5")
                .help("When to use terminal colors"),
        )
        .arg(
            Arg::with_name("query")
                .help("The query to run")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::with_name("param")
                .multiple(true)
                .value_name("param")
                .help("Query parameters"),
        )
}

fn run() -> AppResult<()> {
    let matches = app().get_matches();

    let query = matches
        .value_of("query")
        .ok_or(AppError::general("query is required"))?;

    let param_values = matches
        .values_of("param")
        .map(params::parse_args)
        .map_or(Ok(None), |r| r.map(Some))?;

    let config = core::Config::from_matches(&matches)?;
    core::run_query(config, query, param_values)
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{}", err);
    }
}
