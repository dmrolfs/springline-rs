use clap::Parser;
use console::style;
use proctor::tracing::{get_subscriber, init_subscriber};
use springline::settings::CliOptions;
use springline_explorer::AppMenu;

fn main() {
    let app_name = std::env::args().next().unwrap();
    let subscriber = get_subscriber(app_name.as_str(), "warn", std::io::stdout);
    init_subscriber(subscriber);

    let main_span = tracing::info_span!("main");
    let _main_span_guard = main_span.enter();

    let options: CliOptions = CliOptions::parse();
    let mut app = AppMenu::new(options).expect("failed to create application menu");

    eprintln!(
        "\n{} {}!",
        style("Welcome to the").bold(),
        style("Springline Policy Explorer").green().bold()
    );
    if let Err(err) = app.interact() {
        eprintln!("{} failed: {}", app_name, err);
    }
}
