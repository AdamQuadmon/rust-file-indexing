use env_logger::{Builder, Env};
use polars::prelude::{col, IntoLazy, SortMultipleOptions};
use std::path::Path;

use rust_folder_analysis::utils::loading_saving::{get_path_index, to_polars_df};

#[allow(unused)]
use log::{error, info, warn};

fn main() {
    Builder::from_env(Env::default().default_filter_or("info")).init();

    let root_path = Path::new(r"D:\");
    let index_path = Path::new("results/index.csv");

    let path_index = get_path_index(root_path, index_path);

    let df = to_polars_df(path_index).expect("Failed to convert to Polars df");

    println!(
        "{}",
        df.clone()
            .lazy()
            .select([col("name"), col("size")])
            .sort(
                ["size"],
                SortMultipleOptions::new().with_order_descending(true)
            )
            .collect()
            .expect("Polars failed")
    );
}
