# zstd-lines
A very simple crate to perform line-by-line operations on zstd compressed files. It also features a few benefits for working on large datasets:

- Zstd decompression is performed in stream mode for minimal memory usage
- Uses rayon to work on many files in parallel
- Automatically deals with .tar files and strips the tar header

Simply add zstd-lines to your ``Cargo.toml`` configuration:
```toml
[dependencies]
zstd_lines = { git = "https://github.com/uniQIndividual/zstd-lines" }
```

You can then call ``par_zstd_lines()`` on any vector containing a valid path:

```rust
use zstd_lines::ParZstdLines;
use std::path::PathBuf;

fn main() {
    let files = vec![
        PathBuf::from("11140000000-11150000000.jsonl.zst"),
        PathBuf::from("bungo-pgcr.tar.zst"),
    ];

    files.par_zstd_lines(|line| {
        // Implement your logic here
        println!("Decompressed line: ", line);
    });
}
```
zstd-lines tries to automatically detect .tar files by their file extension and treat them as one continuous files omitting all .tar headers (no integrity checks are performed).

You can call ``par_zstd_lines()`` on everything that fulfills ``AsRef<Path>``:
```rust
    let files_as_pathbuf = vec![
        PathBuf::from("11140000000-11150000000.jsonl.zst"),
        PathBuf::from("111150000000-11160000000.jsonl.zst"),
    ];
    let files_as_path = vec![
        Path::new("11140000000-11150000000.jsonl.zst"),
        Path::new("11150000000-11160000000.jsonl.zst"),
    ];
    let files_as_strings = vec![
        "11140000000-11150000000.jsonl.zst",
        "11150000000-11160000000.jsonl.zst",
    ];
```



For an example how to use zstd-lines, look at my other project [zstd-jsonl-filter](https://github.com/uniQIndividual/zstd-jsonl-filter).