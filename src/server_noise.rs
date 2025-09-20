use anyhow::Result;
use kv::{MemTable, NoiseServerAcceptor, ProstServerStream, Service};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:9527";

    // 加载服务器静态密钥（可选）
    let server_key = include_str!("../fixtures_noise/server.key");
    let server_key_bytes = if server_key.is_empty() {
        None
    } else {
        Some(kv::load_key(server_key)?)
    };

    let acceptor = NoiseServerAcceptor::new(server_key_bytes)?;

    let service: Service = Service::new(MemTable::new());
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);
    loop {
        let noise_acceptor = acceptor.clone();
        let (stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);
        let stream = noise_acceptor.accept(stream).await?;
        let stream = ProstServerStream::new(stream, service.clone());
        tokio::spawn(async move { stream.process().await });
    }
}
