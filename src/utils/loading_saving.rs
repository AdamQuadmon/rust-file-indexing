use crate::indexing::indexing::create_index;
use crate::path_data::PathData;
use polars::prelude::*;
use std::time::UNIX_EPOCH;

use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Write},
    path::Path,
};

pub fn save_path_index(file_path: &Path, path_results: &Vec<PathData>) {
    let json_data =
        serde_json::to_string(&path_results).expect("Failed to serialize path results.");

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)
        .expect("Failed to open index file.");

    file.write_all(json_data.as_bytes())
        .expect("Failed to write data to file.");
}

pub fn load_path_index(file_path: &Path) -> Vec<PathData> {
    let file = File::open(file_path).expect("Failed to open index file for reading.");
    let reader = BufReader::new(file);
    let path_results: Vec<PathData> =
        serde_json::from_reader(reader).expect("Failed to deserialize JSON data.");

    path_results
}

pub fn get_path_index(root_path: &Path, index_path: &Path) -> Vec<PathData> {
    if !index_path.exists() {
        let path_results = create_index(root_path);
        save_path_index(index_path, &path_results);
        path_results
    } else {
        load_path_index(index_path)
    }
}

pub fn to_polars_df(path_index: Vec<PathData>) -> Result<DataFrame, PolarsError> {
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
