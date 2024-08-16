#[allow(unused)]
use polars::prelude::*;

use crate::utils::file_operations::print_and_save;

const BYTES_TO_MB: u64 = 1024 * 1024;
const BYTES_TO_GB: u64 = 1024 * 1024 * 1024;

fn total_folder_size(df: &DataFrame) -> u64 {
    df.column("size")
        .expect("Failed to get size column")
        .u64()
        .expect("Failed to convert to u64")
        .sum()
        .expect("Failed to sum")
        / BYTES_TO_GB
}

fn top_n_file_sizes(df: &DataFrame, top_n: u32) -> DataFrame {
    df.clone()
        .lazy()
        .with_columns([(col("size") / lit(BYTES_TO_MB)).alias("size (MB)")])
        .select([col("name"), col("size (MB)"), col("extension"), col("path")])
        .sort(
            ["size (MB)"],
            SortMultipleOptions::new().with_order_descending(true),
        )
        .limit(top_n)
        .collect()
        .expect("Failed to sort DataFrame by size")
}

fn file_size_per_extension(df: &DataFrame) -> DataFrame {
    df.clone()
        .lazy()
        .group_by([col("extension")])
        .agg([col("size").sum().alias("total_size")])
        .with_column((col("total_size") / lit(BYTES_TO_MB)).alias("size (MB)"))
        .select([col("extension"), col("size (MB)")])
        .sort(
            ["size (MB)"],
            SortMultipleOptions::new().with_order_descending(true),
        )
        .collect()
        .expect("Failed to group")
}

fn extension_counts(df: &DataFrame) -> DataFrame {
    df.clone()
        .lazy()
        .group_by([col("extension")])
        .agg([col("extension").count().alias("count")])
        .sort(
            ["count"],
            SortMultipleOptions::new().with_order_descending(true),
        )
        .collect()
        .expect("Failed to count file extensions.")
}

fn largest_folders(df: &DataFrame) -> DataFrame {
    df.clone()
        .lazy()
        .group_by([col("parents")])
        .agg([col("size").sum().alias("total_size")])
        .with_column((col("total_size") / lit(BYTES_TO_MB)).alias("size (MB)"))
        .select([col("parents"), col("size (MB)")])
        .sort(
            ["size (MB)"],
            SortMultipleOptions::new().with_order_descending(true),
        )
        .collect()
        .expect("Failed to sum by parents")
}

pub fn run_analysis(df: DataFrame) {
    println!("{:?}", df.get_column_names());

    let total_folder_size: u64 = total_folder_size(&df);

    let top_n = 100;

    println!("Total folder size: {} GB", total_folder_size);

    print_and_save(&mut top_n_file_sizes(&df, top_n), "top_n_file_sizes.csv");
    print_and_save(
        &mut file_size_per_extension(&df),
        "file_size_per_extension.csv",
    );
    print_and_save(&mut extension_counts(&df), "extension_counts.csv");
    print_and_save(&mut largest_folders(&df), "largest_folders.csv");
}
