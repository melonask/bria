use clap::Parser as _;

fn main() {
    let cli = bria::Cli::parse();

    let worker_threads = if cli.is_ping() {
        0
    } else {
        bria::Config::load_from_path(&cli.config)
            .map(|cfg| cfg.global.worker_threads)
            .unwrap_or(0)
    };

    let mut runtime_builder = tokio::runtime::Builder::new_multi_thread();
    runtime_builder.enable_all();
    if worker_threads > 0 {
        runtime_builder.worker_threads(worker_threads);
    }

    let runtime = match runtime_builder.build() {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("failed to build Tokio runtime: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = runtime.block_on(bria::run(cli)) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
