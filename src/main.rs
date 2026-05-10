fn main() {
    if let Err(err) = cloudcache::cli::run(std::env::args().collect()) {
        eprintln!("cloudcache: {err}");
        std::process::exit(1);
    }
}
