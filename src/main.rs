use env_logger::{Builder, Env};
use std::path::Path;

use rust_folder_analysis::utils::loading_saving::get_path_index_parquet;

#[allow(unused)]
use polars::prelude::*;

#[allow(unused)]
use log::{error, info, warn};

fn main() {
    Builder::from_env(Env::default().default_filter_or("info")).init();

    let root_path = Path::new(r"D:\");
    let index_path = Path::new(r"D:\Desktop\rust-folder-analysis\results\index.parquet");

    let start = std::time::Instant::now();
    let df = get_path_index_parquet(root_path, index_path);
    let duration = start.elapsed();
    info!("Time taken: {:.3?} seconds", duration.as_secs_f64());

    println!("{:?}", df.get_column_names());

    const BYTES_TO_MB: u64 = 1024 * 1024;
    const BYTES_TO_GB: u64 = 1024 * 1024 * 1024;

    println!(
        "Total size: {:?}",
        df.column("size")
            .expect("Failed to get sum")
            .u64()
            .expect("Failed to convert to u64")
            .sum()
            .expect("Failed to sum")
            / BYTES_TO_GB
    );

    let mut results = df
        .lazy()
        .select([col("name"), col("size"), col("extension")])
        .filter(col("extension").str().contains(lit("csv"), false))
        .sort(
            ["size"],
            SortMultipleOptions::new().with_order_descending(true),
        )
        .with_columns([(col("size") / lit(BYTES_TO_MB)).alias("size (MB)")])
        .limit(100)
        .collect()
        .expect("Polars failed");

    println!("Results: {}", results);

    CsvWriter::new(
        &mut std::fs::File::create("results/output.csv").expect("Failed to create file"),
    )
    .include_header(true)
    .with_separator(b',')
    .finish(&mut results)
    .expect("Failed to write df.");
}
