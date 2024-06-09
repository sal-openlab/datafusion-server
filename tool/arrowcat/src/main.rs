#![warn(clippy::pedantic)]

// Shows arrow format file
// Sasaki, Naoki <nsasaki@sal.co.jp> June 9, 2024
//

use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
use std::str;

use arrow::util::pretty::pretty_format_batches;
use clap::Parser;

mod decoder;
mod record_batch;

#[derive(Parser)]
#[clap(author, version, about = "Shows arrow format file", long_about = None)]
struct Args {
    #[clap(long, short = 'b', value_name = "base64", help = "decodes base64")]
    base64: bool,

    #[clap(value_name = "FILE", help = "filename or '-' (stdin)")]
    file: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut buffer = Vec::new();

    if args.file == PathBuf::from("-") {
        let stdin = io::stdin();
        stdin.lock().read_to_end(&mut buffer)?;
    } else {
        let mut file = File::open(args.file)?;
        file.read_to_end(&mut buffer)?;
    };

    if args.base64 {
        buffer = decoder::base64(str::from_utf8(&buffer)?)?;
    }

    match record_batch::create(buffer) {
        Ok(batches) => {
            println!("{}", pretty_format_batches(&batches)?);
        }
        Err(e) => eprintln!("Can not create record batches: {e}"),
    }

    Ok(())
}
