use hex;
use ring::digest::{Context, SHA256};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

pub fn hash_file(file_path: &Path) -> Result<String, std::io::Error> {
    let file = File::open(file_path).expect("Failed to read file");

    let mut reader = BufReader::new(file);
    let mut context = Context::new(&SHA256);
    let mut buffer = [0; 1024];

    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        context.update(&buffer[..count]);
    }

    Ok(hex::encode(context.finish().as_ref()))
}

pub fn hash_iterable<I, T>(iterable: I) -> String
where
    I: IntoIterator<Item = T>,
    T: AsRef<[u8]>,
{
    let mut context = Context::new(&SHA256);

    for item in iterable {
        context.update(item.as_ref());
    }

    let digest = context.finish();
    hex::encode(digest.as_ref())
}
