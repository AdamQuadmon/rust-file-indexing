// #![allow(unused_imports)]
// #![allow(dead_code)]
// #![allow(unused_variables)]

use env_logger::{Builder, Env};
use rust_folder_analysis::indexing::index_processing::create_path_index;
use std::path::Path;

use std::io::{Error, ErrorKind};

use clap::{arg, command, Arg, ArgAction};
use std::path::PathBuf;

use std::env::current_dir;

#[allow(unused)]
use polars::prelude::*;

#[allow(unused)]
use log::{error, info, warn};

// fn polars_analysis(df: DataFrame) {
//     println!("{:?}", df.get_column_names());

//     const BYTES_TO_MB: u64 = 1024 * 1024;
//     const BYTES_TO_GB: u64 = 1024 * 1024 * 1024;

//     println!(
//         "Total size: {:?}",
//         df.column("size")
//             .expect("Failed to get sum")
//             .u64()
//             .expect("Failed to convert to u64")
//             .sum()
//             .expect("Failed to sum")
//             / BYTES_TO_GB
//     );

//     let mut results = df
//         .lazy()
//         .select([col("name"), col("size"), col("extension")])
//         .filter(col("extension").str().contains(lit("csv"), false))
//         .sort(
//             ["size"],
//             SortMultipleOptions::new().with_order_descending(true),
//         )
//         .with_columns([(col("size") / lit(BYTES_TO_MB)).alias("size (MB)")])
//         .limit(100)
//         .collect()
//         .expect("Polars failed");

//     println!("Results: {}", results);

//     CsvWriter::new(
//         &mut std::fs::File::create("results/output.csv").expect("Failed to create file"),
//     )
//     .include_header(true)
//     .with_separator(b',')
//     .finish(&mut results)
//     .expect("Failed to write df.");
// }

/// Checks whether a path exists and whether it is a folder.
fn check_valid_folder_path(path: &str) -> Result<&Path, Error> {
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
        .arg(arg!([index_path] "Folder path to start recursive indexing from.").required(true))
        .arg(
            Arg::new("cache_location")
                .short('c')
                .long("cache_location")
                .help(
                    "Location to save the parquet cache to. Defaults to the executable directory.",
                ),
        )
        .arg(
            Arg::new("metadata")
                .short('m')
                .long("metadata")
                .help("Include metadata in the search")
                .action(ArgAction::SetTrue),
        )
        .get_matches();

    // Folder is required, so Clap will throw an error before this already.
    let index_path = check_valid_folder_path(
        matches
            .get_one::<String>("index_path")
            .expect("Failed to pass index path"),
    )
    .expect("Invalid path given.");

    let cache_path: PathBuf =
        if let Some(cache_location) = matches.get_one::<String>("cache_location") {
            PathBuf::from(
                check_valid_folder_path(&cache_location)
                    .expect("Invalid path given for cache location."),
            )
        } else {
            current_dir().expect("Can't locate executable: cannot save cache.")
        };

    let get_metadata = matches.get_flag("metadata");

    let _df = create_path_index(index_path, &cache_path, get_metadata);
}
