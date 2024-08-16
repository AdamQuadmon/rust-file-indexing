#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use env_logger::{Builder, Env};
use std::path::Path;

use std::io::{Error, ErrorKind};
use std::thread::current;

use rust_folder_analysis::utils::loading_saving::get_path_index_parquet;

use clap::{arg, command, value_parser, Arg, ArgAction, Command};
use std::path::PathBuf;

use std::env::current_dir;

#[allow(unused)]
use polars::prelude::*;

#[allow(unused)]
use log::{error, info, warn};

fn polars_analysis(df: DataFrame) {
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

fn check_path(path: &str) -> Result<&Path, Error> {
    let path = Path::new(path);

    if !path.exists() {
        return Err(Error::new(
            ErrorKind::NotFound,
            format!("The specified path does not exist: {:?}", path),
        ));
    }

    if !path.is_dir() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("The specified path is not a folder: {:?}", path),
        ));
    }

    Ok(path)
}

fn main() {
    Builder::from_env(Env::default().default_filter_or("info")).init();

    let matches = command!()
        .arg(arg!([index_path] "Folder path to start recursive indexing from").required(true))
        .arg(Arg::new("cache_location").short('c').long("cache_location"))
        .arg(
            Arg::new("metadata")
                .short('v')
                .long("metadata")
                .action(ArgAction::SetTrue),
        )
        .get_matches();

    // Folder is required, so Clap will throw an error before this already.
    let index_path = check_path(
        matches
            .get_one::<String>("index_path")
            .expect("Failed to pass index path"),
    )
    .expect("Invalid path given.");

    let cache_location: PathBuf = if let Some(cache_location) =
        matches.get_one::<String>("cache_location")
    {
        PathBuf::from(check_path(&cache_location).expect("Invalid path given for cache location."))
    } else {
        current_dir().expect("Can't locate executable: cannot save cache.")
    };

    println!("Cache location: {:?}", cache_location);

    let get_metadata = matches.get_flag("metadata");

    let start = std::time::Instant::now();
    let df = get_path_index_parquet(index_path, &cache_location);
    let duration = start.elapsed();
    info!("Time taken: {:.3?} seconds", duration.as_secs_f64());
}
