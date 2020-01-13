## Kass

![](https://github.com/jerrypnz/kass/workflows/build/badge.svg)

Command line Cassandra query runner that supports walking and querying
multiple partitions using a simple syntax. Results are printed as JSON
objects that are friendly to tools like `jq`.

### Quick Demo

Given the following table

``` sql
create table mydb.user_click (
  bin text,
  country text,
  url text,
  ip inet,
  clicked_at timestamp,
primary key ((bin, country), url, ip))
with clustering order by (url asc, ip asc);
```

where `bin` is a date string like `2020-01-13`.

You can run multiple queries against all target partitions like this:

``` shell
$ kass -h localhost \
"select bin, country, count(*) from mydb.user_click
 where bin=? and country=? and url='http://myawsome-web-product.com/landing.html'" \
2019-12-01..2020-01-10/1d nz,us,au,cn
```

This will query the `mydb.user_click` table for the counts of user clicks
of the specified URL from countries NZ, US, AU and CN, between
2019-12-01 and 2020-01-10. It walks through all the combinations of
the provided dates (as specified by the range) and countries (comma
separated list) and runs queries against these partitions in parallel
(with configurable parallelism).

Results are encoded in JSON for easier post-processing,
e.g. aggregation using `jq`.

```json
{"bin":"2019-12-01","country":"au","count":1}
{"bin":"2019-12-01","country":"us","count":2}
{"bin":"2019-12-01","country":"cn","count":1}
{"bin":"2019-12-02","country":"au","count":1}
{"bin":"2019-12-02","country":"us","count":2}
{"bin":"2019-12-26","country":"au","count":1}
{"bin":"2019-12-26","country":"us","count":2}
{"bin":"2019-12-27","country":"cn","count":1}
{"bin":"2019-12-31","country":"cn","count":1}
{"bin":"2020-01-07","country":"cn","count":1}
{"bin":"2020-01-08","country":"nz","count":3}
{"bin":"2020-01-09","country":"nz","count":3}
{"bin":"2020-01-08","country":"au","count":1}
{"bin":"2020-01-08","country":"cn","count":1}
{"bin":"2020-01-08","country":"us","count":2}
{"bin":"2020-01-09","country":"us","count":2}
{"bin":"2020-01-09","country":"cn","count":1}
{"bin":"2020-01-09","country":"au","count":1}
```

### Rationale

With Cassandra, table schemas are often designed around the query
pattern and you really need to follow that pattern to be able to
efficiently query the data. This means it is needed to run the same
query against multiple partitions when you want to query the data
differently. In the above example, you can't do something like
`bin>'2019-12-01' and bin<'2020-01-01 and county in ('nz', 'us', 'au',
'cn')` as you would in a SQL database. This is especially common when
working with time series data.

I often find myself writing bash scripts that does essentially the
same thing but uses `cqlsh`. I couldn't find a similar tool so I
decided to make one and also learn Rust in the process.

It is based on the pure Rust Cassandra driver
[cdrs](https://github.com/AlexPikalov/cdrs).

### Installation

#### Using `cargo`

Coming soon

#### Manual

Clone the repo and run

``` shell
cargo install --path .
```

add `--force` if you want to upgrade

``` shell
cargo install --path . --force
```

### Usage

``` shell
$ kass --help
Kass 0.1.0
Cassandra multi-partition query runner

USAGE:
    kass [OPTIONS] <query> [param]...

OPTIONS:
    -C, --color <color>                When to use terminal colors [default: auto]  [possible values:
                                       auto, on, off]
        --help                         Prints help information
    -h, --host <HOST:PORT>             The Cassandra host to connect to
    -P, --parallelism <parallelism>    Max number of parallel queries [default: 5]
        --pretty                       Pretty print JSON
    -V, --version                      Prints version information

ARGS:
    <query>       The query to run
    <param>...    Query parameters
```

More to come


### Examples

Coming soon
