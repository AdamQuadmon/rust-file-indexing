use std::{path::PathBuf, time::SystemTime};

use serde::{Deserialize, Serialize};

pub mod path_data;

#[derive(Debug, Serialize, Deserialize)]
pub struct PathData {
    path: PathBuf,
    name: String,
    stem: Option<String>,
    size: Option<u64>,
    extension: Option<String>,
    created: Option<SystemTime>,
    modified: Option<SystemTime>,
    is_folder: bool,
}
