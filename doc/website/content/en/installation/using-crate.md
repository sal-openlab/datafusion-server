---
title: Using crate for your project
weight: 20
---

{{< toc >}}

## Pre-require (MSRV)

* Rust Toolchain 1.76+ (Edition 2021) from https://www.rust-lang.org

## Add DataFusion Server crate to your Cargo.toml

If you do not need the plugin feature, simply use `cargo add datafusion-server` or edit Cargo.toml manually.

```toml
[dependencies]
datafusion-server = "0.9.2"
```

or enables Python plugin feature:

```toml
[dependencies]
datafusion-server = { version = "0.9.2", features = ["plugin"] }
```

## Example of call the DataFusion Server entry function

```rust
use datafusion_server::settings::Settings;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // making configuration for the DataFusion Server
    let settings = Settings::new()?;

    // executing the DataFusion Server with default configuration
    datafusion_server::execute(settings)?;

    Ok(())
}
```
