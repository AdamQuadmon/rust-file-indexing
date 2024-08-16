// #![allow(unused_imports)]
// #![allow(dead_code)]
// #![allow(unused_variables)]

use clap::{arg, command, Arg, ArgAction};
use env_logger::{Builder, Env};

use rust_folder_analysis::analysis::analysis::run_analysis;
use rust_folder_analysis::indexing::index_processing::create_path_index;
use rust_folder_analysis::utils::file_operations::check_valid_folder_path;

use std::env::current_dir;
use std::path::PathBuf;

#[allow(unused)]
use log::{error, info, warn};

fn main() {
    Builder::from_env(Env::default().default_filter_or("info")).init();

    // CLI options.
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
                .help("Include metadata in the search. This is slower than without metadata.")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("analysis")
                .short('a')
                .long("analysis")
                .help("Run Polars analysis code following the indexing operation.")
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

    // Running index and creating DataFrame
    let df = create_path_index(index_path, &cache_path, get_metadata);

    // Optional Polars analysis on the results.
    if matches.get_flag("analysis") {
        if matches.get_flag("metadata") {
            run_analysis(df);
        } else {
            warn!("Analysis requires metadata flag (-m).")
        }
    }
}
