use anyhow::Result;
use bytes::BytesMut;
use futures::prelude::*;
use prost::Message;
use kv::{CommandRequest, CommandResponse};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    // 连接服务器
    let stream = TcpStream::connect(addr).await?;

    let mut stream = Framed::new(stream, LengthDelimitedCodec::new());

    // 生成一个 HSET 命令
    let cmd = CommandRequest::new_hset("table1", "hello", "world".into());

    let mut buf = BytesMut::new();
    cmd.encode(&mut buf)?;
    // 发送 HSET 命令
    stream.send(buf.freeze()).await?;
    if let Some(Ok(data)) = stream.next().await {
        let res = CommandResponse::decode(&data[..])?;
        info!("Got response {:?}", res);
    }

    Ok(())
}