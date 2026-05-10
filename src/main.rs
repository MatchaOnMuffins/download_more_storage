fn main() {
    match cloudcache::cli::run(std::env::args().collect()) {
        Ok(()) => {}
        Err(cloudcache::cli::CliError::Clap(err)) => err.exit(),
        Err(cloudcache::cli::CliError::Cloud(err)) => {
            eprintln!("cloudcache: {err}");
            std::process::exit(1);
        }
    }
}
