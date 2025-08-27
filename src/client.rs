use anyhow::Result;
use tokio::net::TcpStream;
use tracing::info;
use tracing_subscriber::fmt::init;
use kv::{CommandRequest, ProstClientStream, TlsClientConnector};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let ca_cert = include_str!("../fixtures/ca.cert");
    let client_cert = include_str!("../fixtures/client.cert");
    let client_key = include_str!("../fixtures/client.key");

    let addr = "127.0.0.1:9527";
    // 连接服务器
    let connector = TlsClientConnector::new("kvserver.acme.inc",Some((client_cert,client_key)),Some(ca_cert))?;
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