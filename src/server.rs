use anyhow::Result;
use kv::{MemTable, ProstServerStream, Service, TlsServerAcceptor, YamuxCtrl};
use tokio::net::TcpListener;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:9527";

    let server_cert = include_str!("../fixtures/server.cert");
    let server_key = include_str!("../fixtures/server.key");
    let ca_cert = include_str!("../fixtures/ca.cert");

    let acceptor = TlsServerAcceptor::new(server_cert, server_key, Some(ca_cert))?;

    let service: Service = Service::new(MemTable::new()).into();
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening on {}", addr);
    loop {
        let tls = acceptor.clone();
        let (stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);

        let svc = service.clone();
        tokio::spawn(async move {
            let stream = tls.accept(stream).await.unwrap();
            YamuxCtrl::new_server(stream, None, move |stream| {
                let svc1 = svc.clone();
                async move {
                    let stream = ProstServerStream::new(stream.compat(), svc1.clone());
                    stream.process().await.unwrap();
                    Ok(())
                }
            })
        });
    }
}
