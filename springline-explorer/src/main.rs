use proctor::tracing::{get_subscriber, init_subscriber};
use springline::settings::CliOptions;
use clap::Parser;
use console::style;
use springline_explorer::AppMenu;

fn main() {
    let app_name = std::env::args().nth(0).unwrap();
    let subscriber = get_subscriber(app_name.as_str(), "info", std::io::stdout);
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let options: CliOptions = CliOptions::parse();

    eprintln!("\nWelcome to the {}!", style("Springline Policy Explorer").green().bold());
    let mut menu = AppMenu::new(options).expect("failed to create application menu");
    if let Err(err) = menu.interact() {
        eprintln!("{} failed: {}", app_name, err);
    }
}
