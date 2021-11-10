use proctor::tracing::{get_subscriber, init_subscriber};
use springline::settings::{CliOptions, Settings};
use clap::Parser;
use console::style;
use springline::bin::explorer::{MenuAction, AppMenu};

fn main() {
    let app_name = std::env::args().nth(0).unwrap();
    let subscriber = get_subscriber(app_name, "info", std::io::stdout);
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let options: CliOptions = CliOptions::parse();

    eprintln!("\nWelcome to the {}!", style("Springline Policy Explorer").green().bold());
    let menu = AppMenu::new(options);
    if let Err(err) = menu.interact() {
        eprintln!("{} failed: {}", app_name, err);
    }
}
