use crate::indexing::index_creation::create_index;
use crate::path_data::PathData;
use polars::prelude::*;
use std::time::UNIX_EPOCH;

use std::path::Path;

use crate::utils::file_operations::{_load_path_index_cache, save_path_index_cache};

/// Creates the path index, loads the Polars df, and saves the cache.
pub fn create_path_index(
    index_path: &Path,
    cache_path: &Path,
    get_metadata: bool,
    get_hash: bool,
) -> DataFrame {
    let path_index = create_index(index_path, get_metadata, get_hash);
    let df = to_polars_df(&path_index).expect("Failed to convert to Polars.");
    save_path_index_cache(cache_path, &df);
    df
}

/// Currently unused: can load in from cache if that exists, or create if it doesn't.
pub fn _create_or_from_cache(
    index_path: &Path,
    cache_path: &Path,
    get_metadata: bool,
    get_hash: bool,
) -> DataFrame {
    if !cache_path.exists() {
        create_path_index(index_path, cache_path, get_metadata, get_hash)
    } else {
        _load_path_index_cache(cache_path)
    }
}

/// Conversion of the vectors to a Polars DataFrame for further analysis.
pub fn to_polars_df(path_index: &Vec<PathData>) -> Result<DataFrame, PolarsError> {
    let paths: Vec<String> = path_index
        .iter()
        .map(|d| d.path.to_string_lossy().into_owned())
        .collect();
    let parents: Vec<String> = path_index
        .iter()
        .map(|d| d.parent.to_string_lossy().into_owned())
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
    let hash: Vec<Option<String>> = path_index.iter().map(|d| d.hash.clone()).collect();

    let df = DataFrame::new(vec![
        Series::new("path", paths),
        Series::new("parents", parents),
        Series::new("name", names),
        Series::new("stem", stems),
        Series::new("size", sizes),
        Series::new("extension", extensions),
        Series::new("created", created),
        Series::new("modified", modified),
        Series::new("is_folder", is_folders),
        Series::new("hash", hash),
    ])?;

    Ok(df)
}
