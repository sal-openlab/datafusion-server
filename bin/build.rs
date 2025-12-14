// environment specific build options

fn main() {
    #[cfg(feature = "plugin")]
    {
        use std::{env, path::PathBuf, process::Command};

        println!("cargo:rerun-if-env-changed=PYO3_PYTHON");

        if let Ok(python) = env::var("PYO3_PYTHON") {
            let output = Command::new(&python)
                .args([
                    "-c",
                    "import sysconfig; print(sysconfig.get_config_var('LIBDIR') or '')",
                ])
                .output()
                .expect("failed to run PYO3_PYTHON to query LIBDIR");

            let lib_dir = String::from_utf8(output.stdout)
                .expect("LIBDIR is not valid UTF-8")
                .trim()
                .to_string();

            let lib_dir = PathBuf::from(lib_dir);
            assert!(
                lib_dir.is_dir(),
                "LIBDIR '{}' from PYO3_PYTHON is not a directory",
                lib_dir.display()
            );

            #[cfg(any(target_os = "linux", target_os = "macos"))]
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
        } else {
            // for aarch64-darwin-apple Xcode command line tools
            #[cfg(target_os = "macos")]
            {
                println!(
                    "cargo:rustc-link-search=native=/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/current/lib"
                );
                println!(
                    "cargo:rustc-link-arg=-Wl,-rpath,/Library/Developer/CommandLineTools/Library/Frameworks"
                );
            }
        }
    }
}
