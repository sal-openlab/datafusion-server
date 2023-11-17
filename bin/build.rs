// environment specific build options

fn main() {
    #[cfg(feature = "plugin")]
    {
        // for aarch64-darwin-apple Xcode command line tools
        #[cfg(target_os = "macos")]
        {
            println!(
                "cargo:rustc-link-search=native=/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/current/lib"
            );
            // same as
            // ```sh
            // export RUSTFLAGS='-C link-args=-Wl,-rpath,/Library/Developer/CommandLineTools/Library/Frameworks'
            // ```
            println!(
                "cargo:rustc-link-arg=-Wl,-rpath,/Library/Developer/CommandLineTools/Library/Frameworks"
            );
        }

        #[cfg(target_os = "linux")]
        {
            println!("cargo:rustc-link-search=native=/opt/python/lib");
        }
    }
}
