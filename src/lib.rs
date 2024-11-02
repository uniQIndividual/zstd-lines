/*!
This crate provides a simple function to easily process zstd compressed files line-by-line.
All you need is a vector of files and a closure which processes a single line.
zstd_lines uses the zstd stream decoder to easily process even gigantic files (note that a very long files will still need to be held in memory).
Furthermore zstd_lines uses rayon to process your files in parallel.

Simply add this crate as a dependency:
```toml
[dependencies]
zstd_lines = { git = "https://github.com/uniQIndividual/zstd-lines" }
```

You can then call ``par_zstd_lines()`` on a vector of files:

```rust
use zstd_lines::ParZstdLines;
use std::path::PathBuf;

let files = vec![PathBuf::from("file.jsonl.zst"), PathBuf::from("file.jsonl.tar.zst")];
files.par_zstd_lines(|line, path| {
    println!("Decompressed line: {} in {:?}", line, path);
});
```
*/

use rayon::prelude::*;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;
use zstd::stream::read::Decoder;

const TAR_BLOCK_SIZE: usize = 512;

/// Process zstd compressed files line-by-line and in parallel using stream decompression.
///
/// # Arguments
///
/// * `line_handler` - A function or closure that will handle each decompressed line.
///
/// # Example
/// ```
/// use zstd_lines::ParZstdLines;
/// use std::path::PathBuf;
///
/// let files = vec![PathBuf::from("file.jsonl.zst"), PathBuf::from("file.jsonl.tar.zst")];
/// files.par_zstd_lines(|line, path| {
///     println!("Decompressed line: {} in {:?}", line, path);
/// });
/// ```

pub trait ParZstdLines {
    /// Process each line in zstd compressed files in parallel using stream decompression.
    /// Can be called on a vector of ``AsRef<Path>``, e.g. ``Path``, ``PathBuf``, ``String`` and ``str``
    /// It will attempt to treat .tar files as one continuous file, omitting all tar headers.
    /// 
    /// # Arguments
    ///
    /// * `line_handler` - A function or closure that will handle each decompressed line.
    ///
    /// # Example
    /// ```
    /// let files = vec![PathBuf::from("file.jsonl.zst"), PathBuf::from("file.jsonl.tar.zst")];
    /// files.par_zstd_lines(|line, path| {
    ///     println!("Decompressed line: {} in {:?}", line, path);
    /// });
    /// ```
    fn par_zstd_lines<F>(&self, line_handler: F)
    where
        F: Fn(String, &Path) + Sync + Send;
}

impl<T> ParZstdLines for Vec<T>
where
    T: AsRef<Path> + Sync,
{
    fn par_zstd_lines<F>(&self, line_handler: F)
    where
        F: Fn(String, &Path) + Sync + Send,
    {
        self.par_iter().for_each(|path| {
            let path = path.as_ref();
            if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
                if stem.ends_with(".tar") {
                    // Handle as .tar.zst file
                    if let Err(e) = process_tar_zstd_file(path, &line_handler) {
                        eprintln!("Failed to process tar.zst file {}: {}", path.display(), e);
                    }
                } else {
                    // Handle as regular .zst files with a faster algorithm
                    if let Err(e) = process_zstd_file(path, &line_handler) {
                        eprintln!("Failed to process zst file {}: {}", path.display(), e);
                    }
                }
            }
        });
    }
}

/// Process a regular zstd-compressed file, passing each line to the line handler function.
fn process_zstd_file<F>(path: &Path, line_handler: &F) -> io::Result<()>
where
    F: Fn(String, &Path) + Sync + Send,
{
    let file = File::open(path)?;
    let decoder = Decoder::new(file)?;
    let reader = BufReader::new(decoder);

    for line in reader.lines() {
        match line {
            Ok(content) => line_handler(content, path),
            Err(e) => eprintln!("Error reading line from {}: {}", path.display(), e),
        }
    }

    Ok(())
}

/// Process a tar file line by line, skipping TAR headers and handling file boundaries.
fn process_tar_zstd_file<F>(path: &Path, line_handler: &F) -> io::Result<()>
where
    F: Fn(String, &Path) + Sync + Send,
{
    let file = File::open(path)?;
    let mut decoder = Decoder::new(file)?;

    let mut buffer = [0; TAR_BLOCK_SIZE];
    let mut remainder = Vec::new(); // We want to delay working with Strings as long as possible

    loop {
        let bytes_read = decoder.read(&mut buffer)?;

        if bytes_read == 0 {
            break;
        }

        // Check if the current 512-byte block is a TAR header indicating a new file
        if is_tar_header(&buffer) {
            // Send the remainder as a line if not empty
            if !remainder.is_empty() {
                if let Ok(line) = String::from_utf8(remainder.clone()) {
                    line_handler(line, path);
                }
                remainder.clear();
            }
            continue;
        }

        let mut offset = 0;
        // Iterate over the buffer, identifying newlines and storing the remainder
        for i in 0..bytes_read {
            if buffer[i] == b'\n' {
                // Found a newline, extract the line
                let end = i;
                let mut line_bytes = remainder.clone(); // Include previous remainder
                line_bytes.extend_from_slice(&buffer[offset..end]);
                if let Ok(line) = String::from_utf8(line_bytes) {
                    line_handler(line, path);
                }
                remainder.clear();
                offset = i + 1;
            }
        }

        // Store any remaining bytes after the last newline
        if offset < bytes_read {
            remainder.extend_from_slice(&buffer[offset..bytes_read]);
        }
    }

    // Process any remaining content in remainder as a final line
    if !remainder.is_empty() {
        if let Ok(line) = String::from_utf8(remainder) {
            line_handler(line, path);
        }
    }

    Ok(())
}

/// Check if the provided 512-byte block is a TAR header by examining expected fields.
fn is_tar_header(block: &[u8]) -> bool {
    if block.len() != TAR_BLOCK_SIZE {
        return false;
    }
    let ustar_magic = &block[257..262];
    ustar_magic == b"ustar"
}
