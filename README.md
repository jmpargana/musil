# rafka

## TODO:
- [ ] gzip
- [ ] Formatting enum

```rs
enum Format {
    Json,
    Hex,
    Text,
    Verbose
}

struct PacketDisplay {
    buf: Vec<u8>,
    format: Format
}

impl std::fmt::Display for PacketDisplay {
    fn fmt(&self, &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.format {
            Format::Json => {}
        }
    }
}
```
