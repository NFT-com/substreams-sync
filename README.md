# Substreams Sink PostgreSQL

This is a command line tool to quickly sync a Substreams with a PostgreSQL database.

### Quickstart

1. Install `substreams-sink-postgres` by using the pre-built binary release [available in the releases page](https://github.com/streamingfast/substreams-sink-postgres/releases). Extract `substreams-sink-postgres` binary into a folder and ensure this folder is referenced globally via your `PATH` environment variable.

    > **Note** Or install from source directly `go install github.com/streamingfast/substreams-sink-postgres/cmd/substreams-sink-postgres@latest`.

1. Start Docker Compose:

    ```bash
    docker compose up
    ```

    > **Note** Feel free to skip this step if you already have a running Postgres instance accessible, don't forget to update the connection string in the command below.

1. Run the setup command:

    ```bash
    substreams-sink-postgres setup "psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" docs/nftLoader/schema.sql
    ```

    This will connect to the given database pointed by `psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable`, create the tables and indexes specified in the given `<schema_file>`, and will create the required tables to run the sink (e.g. the `cursors` table).

    > **Note** For the sake of idempotency, we recommend that the schema file only contain `create table if not exists` statements.

1. Run the sink

    Compile the [Substreams](./docs/nftLoader/substreams.yaml) tutorial project first:

    ```bash
    cd docs/nftLoader
    cargo build --target wasm32-unknown-unknown --release
    cd ../..
    ```

    > **Note** To connect to Substreams you will need an authentication token, follow this [guide](https://substreams.streamingfast.io/reference-and-specs/authentication) to obtain one.

    ```shell
    substreams-sink-postgres run \
        "psql://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable" \
        "mainnet.eth.streamingfast.io:443" \
        "./docs/nftLoader/substreams.yaml" \
        db_out
    ```

### Output Module

To be accepted by `substreams-sink-postgres`, your module output's type must be a [sf.substreams.sink.database.v1.DatabaseChanges](https://github.com/streamingfast/substreams-database-change/blob/develop/proto/substreams/sink/database/v1/database.proto#L7) message. The Rust crate [substreams-data-change](https://github.com/streamingfast/substreams-database-change) contains bindings and helpers to implement it easily. Some project implementing `db_out` module for reference:
- [substreams-eth-block-meta](https://github.com/streamingfast/substreams-eth-block-meta/blob/master/src/lib.rs#L35) (some helpers found in [db_out.rs](https://github.com/streamingfast/substreams-eth-block-meta/blob/master/src/db_out.rs#L6))

By convention, we name the `map` module that emits [sf.substreams.sink.database.v1.DatabaseChanges](https://github.com/streamingfast/substreams-database-change/blob/develop/proto/substreams/sink/database/v1/database.proto#L7) output `db_out`.

### PostgreSQL DSN

The connection string is provided using a simple string format respecting the URL specification. The DSN format is:

```
psql://<user>:<password>@<host>/<dbname>[?<options>]
```

Where `<options>` is URL query parameters in `<key>=<value>` format, multiple options are separated by `&` signs. Supported options can be seen [on libpq official documentation](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS). The options `<user>`, `<password>`, `<host>` and `<dbname>` should **not** be passed in `<options>` as they are automatically extracted from the DSN URL.

Moreover, the `schema` option key can be used to select a particular schema within the `<dbname>` database.

### Authentication
export STREAMINGFAST_KEY=${INSERT_KEY}
export SUBSTREAMS_API_TOKEN=$(curl https://auth.dfuse.io/v1/auth/issue -s --data-binary '{"api_key":"'$STREAMINGFAST_KEY'"}' | jq -r .token)