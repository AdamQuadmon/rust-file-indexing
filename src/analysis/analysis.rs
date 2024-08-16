#[allow(unused)]
use polars::prelude::*;

pub fn run_analysis(df: DataFrame) {
    println!("{:?}", df.get_column_names());

    const BYTES_TO_MB: u64 = 1024 * 1024;
    const BYTES_TO_GB: u64 = 1024 * 1024 * 1024;

    println!(
        "Total size: {:?}",
        df.column("size")
            .expect("Failed to get sum")
            .u64()
            .expect("Failed to convert to u64")
            .sum()
            .expect("Failed to sum")
            / BYTES_TO_GB
    );

    let mut results = df
        .lazy()
        .select([col("name"), col("size"), col("extension")])
        .filter(col("extension").str().contains(lit("csv"), false))
        .sort(
            ["size"],
            SortMultipleOptions::new().with_order_descending(true),
        )
        .with_columns([(col("size") / lit(BYTES_TO_MB)).alias("size (MB)")])
        .limit(100)
        .collect()
        .expect("Polars failed");

    println!("Results: {}", results);

    CsvWriter::new(
        &mut std::fs::File::create("results/output.csv").expect("Failed to create file"),
    )
    .include_header(true)
    .with_separator(b',')
    .finish(&mut results)
    .expect("Failed to write df.");
}
