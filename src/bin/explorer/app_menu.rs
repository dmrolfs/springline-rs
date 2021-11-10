use springline::settings::CliOptions;
use springline::settings::Settings;

pub struct AppMenu {
    pub options: CliOptions,
    pub settings: Option<Settings>,
}

impl AppMenu {
    pub fn new(options: CliOptions) -> Self {
        Self { options, }
    }

    pub fn interact(&mut self) -> anyhow::Result<()> {

    }
}