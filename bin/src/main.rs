// main.rs: datafusion-server main

use std::path::PathBuf;

use clap::Parser;
use datafusion_server::settings::Settings;

#[derive(Parser)]
#[clap(author, version, about = "Arrow and other large datasets web server", long_about = None)]
struct Args {
    #[clap(
        long,
        value_parser,
        short = 'f',
        value_name = "FILE",
        help = "Configuration file",
        default_value = "./config.toml"
    )]
    config: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let settings = Settings::new_with_file(&args.config)?;
    datafusion_server::execute(settings)?;
    Ok(())
}
