#![allow(unused_variables)]
#![allow(unused)]

use std::fs::{metadata, DirEntry};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use rust_folder_analysis::path_data::PathData;
use std::io::{Error, ErrorKind};

use std::fs::read_dir;

fn path_os_str_to_string(path_os_str: Option<&std::ffi::OsStr>) -> Option<String> {
    match path_os_str {
        Some(path) => match path.to_str() {
            Some(path) => Some(path.to_string()),
            None => None,
        },
        None => None,
    }
}

fn construct_entry(dir_entry: &DirEntry) -> Result<PathData, Error> {
    let path = dir_entry.path();

    let path_buf = path.to_path_buf();

    // This works for both files and folders.
    // If it fails, something is really wrong.
    let name = path
        .file_name()
        .ok_or_else(|| Error::new(ErrorKind::NotFound, "Failed to access path name"))?
        .to_str()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "Failed to convert to str"))?
        .to_string();

    // Stem is only for files.
    let stem = path_os_str_to_string(path.file_stem());

    // Getting metadata options.
    let (size, created, modified) = match path.metadata() {
        Ok(metadata) => {
            println!("Metadata: {:?}", metadata);
            let size = Some(metadata.len());

            let created: Option<SystemTime> = match metadata.created() {
                Ok(value) => Some(value),
                Err(_) => None,
            };

            let modified: Option<SystemTime> = match metadata.modified() {
                Ok(value) => Some(value),
                Err(_) => None,
            };

            (size, created, modified)
        }
        Err(_) => (None, None, None),
    };

    let extension = path_os_str_to_string(path.extension());

    let is_folder = path.is_dir();

    Ok(PathData::new(
        path_buf, name, stem, size, extension, created, modified, is_folder,
    ))
}

fn build_index(folder_path: &Path) {
    match read_dir(folder_path) {
        Ok(folder_contents) => {
            for path in folder_contents {
                match path {
                    Ok(dir_entry) => {
                        let index_entry_result = construct_entry(&dir_entry);

                        if let Ok(index_entry) = index_entry_result {
                            println!("Index entry: {:?}", index_entry);
                        }
                    }
                    Err(e) => {
                        println!("Error: {:?}", e);
                    }
                }
            }
        }
        Err(_) => {
            println!("Failed to read folder.")
        }
    }
}

fn main() {
    let trial_path = Path::new(r"D:\Downloads\ai-prompt-runner");

    build_index(trial_path);
}
