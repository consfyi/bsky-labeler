use atrium_api::types::TryIntoUnknown as _;
use atrium_crypto::keypair::Did as _;
use std::io::Write as _;

#[derive(serde::Deserialize)]
struct Config {
    bsky_username: String,
    bsky_plc_password: String,
    pds: String,
    labeler_endpoint: String,
    keypair_path: String,
    unregister_labeler: bool,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::init();

    let config: Config = config::Config::builder()
        .add_source(config::File::with_name("config.toml"))
        .set_default("keypair_path", "signing.key")?
        .set_default("unregister_labeler", false)?
        .build()?
        .try_deserialize()?;

    let keypair =
        atrium_crypto::keypair::Secp256k1Keypair::import(&std::fs::read(&config.keypair_path)?)?;

    let session = atrium_api::agent::atp_agent::CredentialSession::new(
        atrium_xrpc_client::reqwest::ReqwestClient::new(&config.pds),
        atrium_api::agent::atp_agent::store::MemorySessionStore::default(),
    );
    session
        .login(&config.bsky_username, &config.bsky_plc_password)
        .await?;
    let agent = atrium_api::agent::Agent::new(session);

    agent
        .api
        .com
        .atproto
        .identity
        .request_plc_operation_signature()
        .await?;

    println!("Updating PLC for {}.", config.bsky_username);

    write!(std::io::stdout(), "PLC token (check your email): ")?;
    std::io::stdout().flush()?;

    let mut plc_token = String::new();
    std::io::stdin().read_line(&mut plc_token)?;
    plc_token = plc_token.trim_end().to_string();

    let creds = agent
        .api
        .com
        .atproto
        .identity
        .get_recommended_did_credentials()
        .await?
        .data;

    let mut operation: atrium_api::com::atproto::identity::sign_plc_operation::Input =
        atrium_api::com::atproto::identity::sign_plc_operation::InputData {
            also_known_as: creds.also_known_as,
            rotation_keys: creds.rotation_keys,
            services: creds.services,
            verification_methods: creds.verification_methods,
            token: Some(plc_token),
        }
        .into();

    let Some(atrium_api::types::Unknown::Object(verification_methods)) =
        operation.verification_methods.as_mut()
    else {
        unreachable!();
    };
    if !config.unregister_labeler {
        verification_methods.insert(
            "atproto_label".to_string(),
            ipld_core::ipld::Ipld::String(keypair.did())
                .try_into()
                .unwrap(),
        );
    } else {
        verification_methods.remove("atproto_label");
    }

    let Some(atrium_api::types::Unknown::Object(services)) = operation.services.as_mut() else {
        unreachable!();
    };
    if !config.unregister_labeler {
        services.insert(
            "atproto_labeler".to_string(),
            ipld_core::ipld::Ipld::Map(std::collections::BTreeMap::from([
                (
                    "type".to_string(),
                    ipld_core::ipld::Ipld::String("AtprotoLabeler".to_string()),
                ),
                (
                    "endpoint".to_string(),
                    ipld_core::ipld::Ipld::String(config.labeler_endpoint),
                ),
            ]))
            .try_into()
            .unwrap(),
        );
    } else {
        services.remove("atproto_labeler");
    }

    let plc_op = agent
        .api
        .com
        .atproto
        .identity
        .sign_plc_operation(operation)
        .await?;

    println!("");
    println!("{}", serde_json::to_string_pretty(&plc_op)?);
    println!("Press ENTER to submit this PLC operation, Ctrl+C to cancel.");
    std::io::stdin().read_line(&mut String::new())?;

    agent
        .api
        .com
        .atproto
        .identity
        .submit_plc_operation(
            atrium_api::com::atproto::identity::submit_plc_operation::InputData {
                operation: plc_op.operation.clone().try_into_unknown().unwrap(),
            }
            .into(),
        )
        .await?;

    Ok(())
}
