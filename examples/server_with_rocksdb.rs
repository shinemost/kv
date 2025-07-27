use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::prelude::*;
use kv::{CommandRequest, CommandResponse, Service, Rocksdb};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let service: Service =
        Service::new(Rocksdb::new("/tmp/kvserver/rocksdb")).fn_before_send(|res: &mut CommandResponse| {
            match res.message.as_ref() {
                "" => res.message = "altered. Original message is empty.".into(),
                s => res.message = format!("altered: {}", s),
            }
            None // 返回 None 表示继续处理
        });
    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;
    info!("Start listening to {}", addr);
    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Client {:?} connected", addr);
        let svc = service.clone();
        tokio::spawn(async move {
            let mut stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();
            while let Some(Ok(cmd)) = stream.next().await {
                let res = svc.execute(cmd);
                if let Err(e) = stream.send(res).await {
                    info!("Failed to send response to client {:?}: {}", addr, e);
                    break;
                }
            }
            info!("Client {:?} disconnected", addr);
        });
    }
}
