// main.rs: datafusion-server main

use clap::Parser;
use std::path::PathBuf;

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

fn main() {
    let args = Args::parse();
    datafusion_server::execute(&args.config);
}
