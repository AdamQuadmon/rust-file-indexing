#![allow(unused_variables)]
#![allow(unused)]

use std::fs::{metadata, DirEntry};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use env_logger::{Builder, Env};
use log::{error, info, warn};

use rust_folder_analysis::path_data::PathData;
use std::io::{Error, ErrorKind};

use rayon::prelude::*;
use std::fs;
use std::sync::{Arc, Mutex};

use std::fs::read_dir;

/// Converts Option<&OsStr> to Option<String>.
/// We can't save a reference in a struct so we need to do this instead.
/// If any conversions fail, this will return a None.
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

    // PathBuf to save in the struct.
    let path_buf = path.to_path_buf();

    // This works for both files and folders.
    // If it fails, something is really wrong, so this will return an Error.
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

    // Extension from OsStr to String.
    let extension = path_os_str_to_string(path.extension());

    let is_folder = path.is_dir();

    // Creating a result.
    Ok(PathData::new(
        path_buf, name, stem, size, extension, created, modified, is_folder,
    ))
}

fn build_index(
    folder_path: &Path,
    folder_queue: &mut Vec<PathBuf>,
    path_results: &mut Vec<PathData>,
) {
    let mut index = 0;

    match read_dir(folder_path) {
        Ok(folder_contents) => {
            for path in folder_contents {
                match path {
                    Ok(dir_entry) => {
                        // Turning everything into a struct based on the entry.
                        let index_entry_result = construct_entry(&dir_entry);

                        if let Ok(index_entry) = index_entry_result {
                            let entry_path = index_entry.path.to_owned();

                            // We need to save to two separate places so this clone is necessary if we have a folder.
                            if index_entry.is_folder {
                                folder_queue.push(entry_path);
                            }

                            // Saving to the index reference vector.
                            path_results.push(index_entry);
                        }
                    }
                    Err(e) => {
                        error!("Failed to read path entry: {:?}", e);
                    }
                }
            }
        }
        Err(e) => {
            error!("Failed to read folder {:?}: {}.", folder_path, e)
        }
    }
}

fn main() {
    Builder::from_env(Env::default().default_filter_or("info")).init();

    let trial_path = Path::new(r"D:\");

    let folder_queue = Arc::new(Mutex::new(vec![trial_path.to_path_buf()]));
    let path_results = Arc::new(Mutex::new(Vec::<PathData>::new()));

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(20)
        .build()
        .unwrap();

    pool.scope(|s| {
        loop {
            let folders = {
                let mut queue = folder_queue.lock().unwrap();
                if queue.is_empty() {
                    break;
                }
                // Extract the folders to process in this iteration
                queue.drain(..).collect::<Vec<_>>()
            };

            // Process folders in parallel
            folders.into_par_iter().for_each(|folder_path| {
                let mut new_folders = Vec::new();
                let mut results = Vec::new();

                build_index(&folder_path, &mut new_folders, &mut results);

                // Safely update the shared folder_queue and path_results
                {
                    let mut queue = folder_queue.lock().unwrap();
                    queue.extend(new_folders);
                }
                {
                    let mut results_guard = path_results.lock().unwrap();
                    results_guard.extend(results);
                }
            });
        }
    });

    let results = Arc::try_unwrap(path_results).unwrap().into_inner().unwrap();

    info!("Results: {}", results.len());
}
