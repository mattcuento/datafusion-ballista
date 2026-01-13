<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Ballista Examples

This directory contains examples for executing distributed queries with Ballista.

## Standalone Examples

The standalone example is the easiest to get started with. Ballista supports a standalone mode where a scheduler
and executor are started in-process.

```bash
cargo run --example standalone_sql --features="ballista/standalone"
```

### Source code for standalone SQL example

```rust
use ballista::{
    extension::SessionConfigExt,
    prelude::*
};
use datafusion::{
    execution::{options::ParquetReadOptions, SessionStateBuilder},
    prelude::{SessionConfig, SessionContext},
};

#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(1)
        .with_ballista_standalone_parallelism(2);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::standalone_with_state(state).await?;

    let test_data = test_util::examples_test_data();

    // register parquet file with the execution context
    ctx.register_parquet(
        "test",
        &format!("{test_data}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    let df = ctx.sql("select count(1) from test").await?;

    df.show().await?;
    Ok(())
}

```

## Distributed Examples

For background information on the Ballista architecture, refer to
the [Ballista README](../ballista/client/README.md).

### Start a standalone cluster

From the root of the project, build release binaries.

```bash
cargo build --release
```

Start a Ballista scheduler process in a new terminal session.

```bash
RUST_LOG=info ./target/release/ballista-scheduler
```

Start one or more Ballista executor processes in new terminal sessions. When starting more than one
executor, a unique port number must be specified for each executor.

```bash
RUST_LOG=info ./target/release/ballista-executor -c 2 -p 50051
RUST_LOG=info ./target/release/ballista-executor -c 2 -p 50052
```

### Running the examples

The examples can be run using the `cargo run --bin` syntax.

### Distributed SQL Example

```bash
cargo run --release --example remote-sql
```

#### Source code for distributed SQL example

```rust
use ballista::{extension::SessionConfigExt, prelude::*};
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{CsvReadOptions, SessionConfig, SessionContext},
};

/// This example demonstrates executing a simple query against an Arrow data source (CSV) and
/// fetching results, using SQL
#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_ballista_job_name("Remote SQL Example");

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    let test_data = test_util::examples_test_data();

    ctx.register_csv(
        "test",
        &format!("{test_data}/aggregate_test_100.csv"),
        CsvReadOptions::new(),
    )
    .await?;

    let df = ctx
        .sql(
            "SELECT c1, MIN(c12), MAX(c12) \
        FROM test \
        WHERE c11 > 0.1 AND c11 < 0.9 \
        GROUP BY c1",
        )
        .await?;

    df.show().await?;

    Ok(())
}
```

### Distributed DataFrame Example

```bash
cargo run --release --example remote-dataframe
```

#### Source code for distributed DataFrame example

```rust
use ballista::{extension::SessionConfigExt, prelude::*};
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{col, lit, ParquetReadOptions, SessionConfig, SessionContext},
};

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results, using the DataFrame trait
#[tokio::main]
async fn main() -> Result<()> {
    let config = SessionConfig::new_with_ballista().with_target_partitions(4);

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();

    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    let test_data = test_util::examples_test_data();
    let filename = format!("{test_data}/alltypes_plain.parquet");

    let df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?
        .select_columns(&["id", "bool_col", "timestamp_col"])?
        .filter(col("id").gt(lit(1)))?;

    df.show().await?;

    Ok(())
}
```

### Distributed Substrait Example

```bash
cargo run --release --example remote-substrait
```

#### Source code for distributed Substrait example

This example demonstrates serializing a DataFusion LogicalPlan with in-memory data to Substrait
format and executing it on a remote Ballista scheduler.

**Note:** File-based table providers (parquet, csv, etc.) don't currently serialize properly to
Substrait with embedded file paths. Use in-memory data (VALUES clause) or register tables on the
scheduler beforehand.

```rust
use ballista::datafusion::common::{DataFusionError, Result};
use ballista::prelude::SessionContextExt;
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::execute_query_params::Query::SubstraitPlan;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use ballista_core::serde::protobuf::{ExecuteQueryParams, execute_query_result};
use ballista_core::utils::{GrpcClientConfig, create_grpc_client_connection};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_substrait::serializer::serialize_bytes;

#[tokio::main]
async fn main() -> Result<()> {
    // Now create the remote context for execution
    let config = SessionConfig::new_with_ballista()
        .with_target_partitions(4)
        .with_ballista_job_name("Remote substrait Example");
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

    // Serialize SQL into encoded Substrait plan. Substitute with your favorite Substrait producer.
    let substrait_bytes = serialize_bytes("SELECT 'double_field', 'string_field' FROM (VALUES (1.5, 'foo'), (2.5, 'bar'), (3.5, 'baz')) AS t(double_field, string_field)", &ctx).await?;

    let connection = create_grpc_client_connection(
        "df://localhost:50050".to_owned(),
        &GrpcClientConfig::default(),
    )
        .await
        .expect("Error creating client");
    let mut scheduler = SchedulerGrpcClient::new(connection);

    let execute_query_params = ExecuteQueryParams {
        session_id: ctx.session_id(),
        settings: vec![],
        operation_id: uuid::Uuid::now_v7().to_string(),
        // Substrait plan available as a plan type!
        query: Some(SubstraitPlan(substrait_bytes)),
    };
    let response = scheduler
        .execute_query(execute_query_params)
        .await
        .expect("Error executing query");

    match response.into_inner().result.unwrap() {
        execute_query_result::Result::Success(_) => {
            println!("Query executed successfully!");
            Ok(())
        }
        execute_query_result::Result::Failure(failure) => Err(
            DataFusionError::Execution(format!("Failed to execute query: {:?}", failure)),
        ),
    }
}
```
