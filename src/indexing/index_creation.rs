#[allow(unused)]
use log::{error, info, warn};
use rayon::{max_num_threads, prelude::*};

use crate::path_data::PathData;

use std::fs::read_dir;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};

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

/// Takes a &Path and extracts necessary information from the current path to populate PathData.
/// Works for both folders and files.
fn construct_entry(path: &Path, get_metadata: bool) -> Result<PathData, Error> {
    // PathBuf to save in the struct.
    let path_buf = path.to_path_buf();

    let parent = match path.parent() {
        Some(parent) => parent,
        None => Path::new(""),
    }
    .to_path_buf();

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
    let (size, created, modified) = if get_metadata {
        match path.metadata() {
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
        }
    } else {
        (None, None, None)
    };

    // Extension from OsStr to String.
    let extension = path_os_str_to_string(path.extension());

    let is_folder = path.is_dir();

    // Creating a result.
    Ok(PathData::new(
        path_buf, parent, name, stem, size, extension, created, modified, is_folder,
    ))
}

/// Analyzes the contents of a folder, returning nested folders as well as paths found.
fn index_folder(
    folder_path: &Path,
    folder_queue: &mut Vec<PathBuf>,
    path_results: &mut Vec<PathData>,
    get_metadata: bool,
) {
    match read_dir(folder_path) {
        Ok(folder_contents) => {
            for path in folder_contents {
                match path {
                    Ok(dir_entry) => {
                        // Turning everything into a struct based on the entry.
                        let index_entry_result = construct_entry(&dir_entry.path(), get_metadata);

                        if let Ok(index_entry) = index_entry_result {
                            // We need to save to two separate places so this is necessary only if we have a folder.
                            if index_entry.is_folder {
                                folder_queue.push(index_entry.path.to_owned());
                            }

                            // Saving to the index reference vector.
                            path_results.push(index_entry);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to read path entry: {:?}", e);
                    }
                }
            }
        }
        Err(e) => {
            warn!("Failed to read folder {:?}: {}.", folder_path, e)
        }
    }
}

/// Parallel processing code with a variable number of threads (default: max_num_threads() / 2, with a maximum of 20).
/// Discovers folders, appends those to a shared queue, which the thread pool allocates workers to.
pub fn create_index(index_path: &Path, get_metadata: bool) -> Vec<PathData> {
    info!("Starting indexing at {:?}", index_path);
    let start = Instant::now();

    let folder_queue = Arc::new(Mutex::new(vec![index_path.to_path_buf()]));
    let path_index = Arc::new(Mutex::new(Vec::<PathData>::new()));

    // Just in case this is ran on a supercomputer, limiting the number of cores to 20.
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(std::cmp::min(max_num_threads() / 2, 20))
        .build()
        .unwrap();

    pool.scope(|_| {
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

                index_folder(&folder_path, &mut new_folders, &mut results, get_metadata);

                // Safely update the shared folder_queue and path_results
                {
                    let mut queue = folder_queue.lock().unwrap();
                    queue.extend(new_folders);
                }
                {
                    let mut results_guard = path_index.lock().unwrap();
                    results_guard.extend(results);
                }
            });
        }
    });

    // Collecting all the data
    let path_data = Arc::try_unwrap(path_index).unwrap().into_inner().unwrap();

    // Printing some neat statistics
    let duration = start.elapsed();
    let paths_indexed_count = path_data.len();

    info!(
        "Indexed {} paths. Time taken: {:.3?} seconds. ({:.0} paths/s)",
        paths_indexed_count,
        duration.as_secs_f64(),
        (paths_indexed_count as f64 / duration.as_secs_f64())
    );

    path_data
}
