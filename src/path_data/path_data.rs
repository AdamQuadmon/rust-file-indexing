use std::{path::PathBuf, time::SystemTime};

use crate::path_data::PathData;

impl PathData {
    /// Creating a new DataFrame instance.
    pub fn new(
        path: PathBuf,
        parent: PathBuf,
        name: String,
        stem: Option<String>,
        size: Option<u64>,
        extension: Option<String>,
        created: Option<SystemTime>,
        modified: Option<SystemTime>,
        is_folder: bool,
    ) -> Self {
        PathData {
            path,
            parent,
            name,
            stem,
            size,
            extension,
            created,
            modified,
            is_folder,
        }
    }
}
