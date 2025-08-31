use anyhow::Result;
use kv::{CommandRequest, NoiseClientConnector, ProstClientStream};
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // 加载客户端静态密钥和服务器公钥（可选）
    let client_key = include_str!("../fixtures_noise/client.key");
    let server_pubkey = include_str!("../fixtures_noise/server.pub");

    let client_key_bytes = if client_key.is_empty() {
        None
    } else {
        Some(kv::load_key(client_key)?)
    };

    let server_pubkey_bytes = if server_pubkey.is_empty() {
        None
    } else {
        Some(kv::load_key(server_pubkey)?)
    };

    let addr = "127.0.0.1:9527";

    // 创建 Noise 连接器
    let connector = NoiseClientConnector::new(client_key_bytes, server_pubkey_bytes)?;

    // 连接服务器
    let stream = TcpStream::connect(addr).await?;
    let stream = connector.connect(stream).await?;

    let mut client = ProstClientStream::new(stream);

    // 生成一个 HSET 命令
    let cmd = CommandRequest::new_hset("table1", "hello", "world".to_string().into());

    // 发送 HSET 命令
    let data = client.execute(cmd).await?;
    info!("Got response {:?}", data);

    Ok(())
}
