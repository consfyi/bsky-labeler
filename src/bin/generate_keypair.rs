use atrium_crypto::keypair::{Did as _, Export as _};

#[derive(serde::Deserialize)]
struct Config {
    keypair_path: String,
}

fn main() -> Result<(), anyhow::Error> {
    env_logger::init();

    let config: Config = config::Config::builder()
        .add_source(config::File::with_name("config.toml"))
        .set_default("keypair_path", "signing.key")?
        .build()?
        .try_deserialize()?;

    let keypair = atrium_crypto::keypair::Secp256k1Keypair::create(&mut rand::thread_rng());
    std::fs::write(config.keypair_path, keypair.export())?;

    println!("{}", keypair.did());

    Ok(())
}
