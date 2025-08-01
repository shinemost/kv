use anyhow::Result;
use futures::prelude::*;
use prost::Message;
use kv::{CommandRequest, CommandResponse, Service, Rocksdb};
use tokio::net::TcpListener;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
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
        let mut stream = Framed::new(stream, LengthDelimitedCodec::new());
        tokio::spawn(async move {
            while let Some(Ok(mut buf)) = stream.next().await {
                let cmd = CommandRequest::decode(&buf[..]).unwrap();
                info!("Got a new command: {:?}", cmd);
                let res = svc.execute(cmd);
                buf.clear();
                res.encode(&mut buf).unwrap();
                if let Err(e) = stream.send(buf.freeze()).await {
                    info!("Failed to send response to client {:?}: {}", addr, e);
                    break;
                }
            }
            info!("Client {:?} disconnected", addr);
        });
    }
}
