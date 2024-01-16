use std::fs;
use std::path::Path;

use datafusion::arrow::json;
// use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
use env_logger::Env;
use tokio::time::Instant;

/// This example demonstrates executing a simple query against an Arrow data source (Parquet) and
/// fetching results
#[tokio::main(flavor = "multi_thread", worker_threads = 12)]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();
    let now = Instant::now();
    let mut query_tasks = Vec::new();
    let num_requests = 100;

    for i in 1..num_requests {
        query_tasks.push(tokio::spawn(compute(i)));
    }

    for task in query_tasks {
        let _ = task.await.expect("waiting failed");
    }
    let end = Instant::now();
    log::info!("Total time for executing {:?}", end - now);
    println!("Total Time elapased {:?}", end - now);
    Ok(())
}

async fn compute(id: u16) -> Result<(), DataFusionError> {
    let start = Instant::now();
    // create local session context
    let ctx = SessionContext::new();

    ctx.register_parquet(
        &format!("pad{}", id),
        &format!("./part_account/{}/file.parquet", id),
        ParquetReadOptions::default(),
    )
    .await
    .expect("Failed to register parquet file");

    let query = (1..=48)
        .map(|i| format!("amount{}", i))
        .map(|column_name| format!("SUM({}) as {}", column_name, column_name))
        .collect::<Vec<String>>()
        .join(", ");

    let sql_query = format!(
        "SELECT stop_date, {} FROM pad{} GROUP BY stop_date",
        query, id
    );
    // execute the query
    let df = ctx.sql(&sql_query).await?;

    // let options = DataFrameWriteOptions::new();
    // let opts = options.with_single_file_output(true);

    let filename = format!("./result{}.json", id);
    let path = Path::new(&filename);
    let file = fs::File::create(path)?;

    let mut writer = json::LineDelimitedWriter::new(file);

    let recs = df.collect().await?;
    for rec in recs {
        writer.write(&rec).expect("Write failed")
    }

    // df.clone()
    //     .write_json(&format!("./result{}", id), opts)
    //     .await?;
    let end = Instant::now();
    log::info!(
        "Finished executing for task {} in time {:?}",
        id,
        end - start
    );
    Ok(())
}
