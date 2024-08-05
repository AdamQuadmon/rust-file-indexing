use crate::path_data::PathData;
use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Write},
    path::Path,
};

pub fn save_index(file_path: &Path, path_results: &Vec<PathData>) {
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

pub fn load_index(file_path: &Path) -> Vec<PathData> {
    let file = File::open(file_path).expect("Failed to open index file for reading.");
    let reader = BufReader::new(file);
    let path_results: Vec<PathData> =
        serde_json::from_reader(reader).expect("Failed to deserialize JSON data.");

    path_results
}
