use std::{env, fs, io, path};

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    let target = path::PathBuf::from(&args[1]);
    let doc = fs::read_to_string(target)?;
    let mut deser = ron::Deserializer::from_str(&doc)?;
    let mut ser = serde_json::Serializer::new(io::stdout());
    let foo = serde_transcode::transcode(&mut deser, &mut ser)?;
    Ok(foo)
}
