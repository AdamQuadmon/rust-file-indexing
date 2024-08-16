use crate::indexing::index_creation::create_index;
use crate::path_data::PathData;
use polars::prelude::*;
use std::time::UNIX_EPOCH;

use std::path::Path;

use crate::utils::file_operations::{load_path_index_cache, save_path_index_cache};

pub fn create_path_index(index_path: &Path, cache_path: &Path, get_metadata: bool) -> DataFrame {
    let path_index = create_index(index_path, get_metadata);
    save_path_index_cache(cache_path, &path_index);
    to_polars_df(&path_index).expect("Failed to convert to Polars.")
}

pub fn create_or_from_cache(index_path: &Path, cache_path: &Path, get_metadata: bool) -> DataFrame {
    if !cache_path.exists() {
        create_path_index(index_path, cache_path, get_metadata)
    } else {
        load_path_index_cache(cache_path)
    }
}

pub fn to_polars_df(path_index: &Vec<PathData>) -> Result<DataFrame, PolarsError> {
    let paths: Vec<String> = path_index
        .iter()
        .map(|d| d.path.to_string_lossy().into_owned())
        .collect();
    let names: Vec<String> = path_index.iter().map(|d| d.name.clone()).collect();
    let stems: Vec<Option<String>> = path_index.iter().map(|d| d.stem.clone()).collect();
    let sizes: Vec<Option<u64>> = path_index.iter().map(|d| d.size).collect();
    let extensions: Vec<Option<String>> = path_index.iter().map(|d| d.extension.clone()).collect();
    let created: Vec<Option<i64>> = path_index
        .iter()
        .map(|d| {
            d.created.and_then(|t| {
                t.duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|dur| dur.as_secs() as i64)
            })
        })
        .collect();
    let modified: Vec<Option<i64>> = path_index
        .iter()
        .map(|d| {
            d.modified.and_then(|t| {
                t.duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|dur| dur.as_secs() as i64)
            })
        })
        .collect();
    let is_folders: Vec<bool> = path_index.iter().map(|d| d.is_folder).collect();

    let df = DataFrame::new(vec![
        Series::new("path", paths),
        Series::new("name", names),
        Series::new("stem", stems),
        Series::new("size", sizes),
        Series::new("extension", extensions),
        Series::new("created", created),
        Series::new("modified", modified),
        Series::new("is_folder", is_folders),
    ])?;

    Ok(df)
}
