use proctor::tracing::{get_subscriber, init_subscriber};
use springline::settings::{CliOptions, Settings};
use clap::Parser;
use console::style;

fn main() {
    let app_name = std::env::args().nth(0).unwrap();
    let subscriber = get_subscriber(app_name, "info", std::io::stdout);
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let options: CliOptions = CliOptions::parse();

    eprintln!("\nWelcome to the {}!", style("Springline Policy Explorer").green().bold());
    let menu = AppMenu::new(options);
    match menu.interact() {
        Ok(()) => eprintln!
    }
}
