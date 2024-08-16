use crate::indexing::index_processing::to_polars_df;
use crate::path_data::PathData;
use polars::prelude::*;

#[allow(unused)]
use log::{error, info, warn};

use std::{
    io::{Error, ErrorKind},
    path::Path,
};

pub fn save_path_index_cache(file_path: &Path, path_index: &Vec<PathData>) {
    let mut df = to_polars_df(path_index).expect("Failed to convert to Polars df");

    let cache_file_path = file_path.join("rust-file-index.parquet");

    info!("Saving cache: {:?}", cache_file_path);

    let mut file =
        std::fs::File::create(cache_file_path).expect("Failed to create parquet index file");
    ParquetWriter::new(&mut file).finish(&mut df).unwrap();
}

pub fn load_path_index_cache(file_path: &Path) -> DataFrame {
    let mut file = std::fs::File::open(file_path).expect("Failed to open file");
    ParquetReader::new(&mut file).finish().unwrap()
}

/// Checks whether a path exists and whether it is a folder.
pub fn check_valid_folder_path(path: &str) -> Result<&Path, Error> {
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
