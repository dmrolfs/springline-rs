use std::io::Write;
use std::str::FromStr;
use std::{fs, io, path};

use clap::{Arg, Command};
use thiserror::Error;

fn main() -> anyhow::Result<()> {
    let matches = Command::new("Springline transcode")
        .arg(
            Arg::new("config")
                .help("Source config document to transcode")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("from")
                .help("from format - defaults to Ron")
                .short('f')
                .long("from")
                .required(false)
                .takes_value(true),
        )
        .arg(Arg::new("to").help("to format").short('t').long("to").takes_value(true))
        .get_matches();

    let target = path::PathBuf::from(matches.value_of("config").unwrap());
    let doc = fs::read_to_string(target)?;
    let from: ConfigFormat = matches.value_of_t("from").unwrap_or(ConfigFormat::Ron);
    let to: ConfigFormat = matches.value_of_t("to").unwrap();
    transcode(doc, from, to)?;
    io::stdout().flush()?;
    Ok(())
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ConfigFormat {
    Json,
    Yaml,
    Ron,
}

#[derive(Error, Debug)]
#[error("Unknown config format: {0}")]
struct UnknownFormat(String);

impl FromStr for ConfigFormat {
    type Err = UnknownFormat;

    fn from_str(rep: &str) -> Result<Self, Self::Err> {
        match rep.to_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "yaml" => Ok(Self::Yaml),
            "ron" => Ok(Self::Ron),
            s => Err(UnknownFormat(s.to_string())),
        }
    }
}

fn transcode(doc: String, from: ConfigFormat, to: ConfigFormat) -> anyhow::Result<()> {
    use ConfigFormat::*;

    match (from, to) {
        (Ron, Json) => {
            let mut deser = ron::Deserializer::from_str(&doc)?;
            let mut ser = serde_json::Serializer::new(io::stdout());
            serde_transcode::transcode(&mut deser, &mut ser)?;
        },
        (Ron, Yaml) => {
            let mut deser = ron::Deserializer::from_str(&doc)?;
            let mut ser = serde_yaml::Serializer::new(io::stdout());
            serde_transcode::transcode(&mut deser, &mut ser)?;
        },
        (Json, Yaml) => {
            let mut deser = serde_json::Deserializer::from_str(&doc);
            let mut ser = serde_yaml::Serializer::new(io::stdout());
            serde_transcode::transcode(&mut deser, &mut ser)?;
        },
        (Json, Ron) => {
            let mut deser = serde_json::Deserializer::from_str(&doc);
            let mut ser = ron::Serializer::new(io::stdout(), None)?;
            serde_transcode::transcode(&mut deser, &mut ser)?;
        },
        (from, to) => unimplemented!("combination not support: {:?} => {:?}", from, to),
    }

    Ok(())
}
