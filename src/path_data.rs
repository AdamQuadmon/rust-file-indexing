use std::{path::PathBuf, time::SystemTime};

use serde::{Deserialize, Serialize};

pub mod path_data;

/// Data collected per path.
/// The optional fields are defined only for files.
#[derive(Debug, Serialize, Deserialize)]
pub struct PathData {
    pub path: PathBuf,
    pub parent: PathBuf,
    pub name: String,
    pub stem: Option<String>,
    pub size: Option<u64>,
    pub extension: Option<String>,
    pub created: Option<SystemTime>,
    pub modified: Option<SystemTime>,
    pub is_folder: bool,
}
