use anyhow::Result;
use std::fs;
use kv::{ClientConfig, ClientTlsConfig, GeneralConfig, ServerConfig, ServerTlsConfig, StorageConfig};

fn main() -> Result<()> {
    const CA_CERT: &str = include_str!("../fixtures/ca.cert");
    const SERVER_CERT: &str = include_str!("../fixtures/server.cert");
    const SERVER_KEY: &str = include_str!("../fixtures/server.key");

    let general_config = GeneralConfig {
        addr: "127.0.0.1:9527".into(),
    };
    let server_config = ServerConfig {
        storage: StorageConfig::SledDb("/tmp/kv_server".into()),
        general: general_config.clone(),
        tls: ServerTlsConfig {
            cert: SERVER_CERT.into(),
            key: SERVER_KEY.into(),
            ca: None,
        },
    };

    fs::write(
        "fixtures/server.conf",
        toml::to_string_pretty(&server_config)?,
    )?;

    let client_config = ClientConfig {
        general: general_config,

        tls: ClientTlsConfig {
            identity: None,
            ca: Some(CA_CERT.into()),
            domain: "kvserver.acme.inc".into(),
        },
    };

    fs::write(
        "fixtures/client.conf",
        toml::to_string_pretty(&client_config)?,
    )?;

    Ok(())
}
