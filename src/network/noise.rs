use bytes::{Buf, BytesMut};
use snow::{Builder, TransportState};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::KvError;

/// KV Server 自己的协议标识 Noise<握手的模式>  <公钥算法>  <对称加密算法>  <哈希算法>。
const PROTOCOL_NAME: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";

/// 存放 Noise Server 配置并提供方法 accept 把底层的协议转换成 Noise
#[derive(Clone)]
pub struct NoiseServerAcceptor {
    config: Arc<NoiseConfig>,
}

/// 存放 Noise Client 配置并提供方法 connect 把底层的协议转换成 Noise
#[derive(Clone)]
pub struct NoiseClientConnector {
    config: Arc<NoiseConfig>,
}

/// Noise 配置
#[derive(Clone)]
pub struct NoiseConfig {
    pub static_key: Option<Vec<u8>>,
    pub remote_public_key: Option<Vec<u8>>,
    pub is_initiator: bool,
}

impl NoiseConfig {
    pub fn new(
        static_key: Option<Vec<u8>>,
        remote_public_key: Option<Vec<u8>>,
        is_initiator: bool,
    ) -> Self {
        Self {
            static_key,
            remote_public_key,
            is_initiator,
        }
    }
}

/// Noise 流包装器
pub struct NoiseStream<S> {
    inner: S,
    state: TransportState,
    read_buffer: BytesMut,   // 存储解密的数据
    encrypt_buffer: Vec<u8>, // 加密临时缓冲区
    decrypt_buffer: Vec<u8>, // 解密临时缓冲区
}

impl<S: AsyncRead + AsyncWrite + Unpin> NoiseStream<S> {
    pub fn new(inner: S, state: TransportState) -> Self {
        Self {
            inner,
            state,
            read_buffer: BytesMut::with_capacity(4096),
            encrypt_buffer: vec![0u8; 4096],
            decrypt_buffer: vec![0u8; 4096],
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for NoiseStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;

        // 首先从缓冲区提供已解密的数据
        if !this.read_buffer.is_empty() {
            let to_copy = std::cmp::min(this.read_buffer.len(), buf.remaining());
            buf.put_slice(&this.read_buffer[..to_copy]);
            this.read_buffer.advance(to_copy);
            return Poll::Ready(Ok(()));
        }

        // 需要从底层流读取加密数据
        let mut encrypted_buf = [0u8; 4096];
        let mut read_buf = ReadBuf::new(&mut encrypted_buf);

        match Pin::new(&mut this.inner).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let n = read_buf.filled().len();
                if n == 0 {
                    return Poll::Ready(Ok(())); // EOF
                }

                // 解密数据
                match this
                    .state
                    .read_message(&encrypted_buf[..n], &mut this.decrypt_buffer)
                {
                    Ok(decrypted_len) => {
                        if decrypted_len == 0 {
                            return Poll::Ready(Ok(()));
                        }

                        // 将解密的数据存入缓冲区
                        this.read_buffer
                            .extend_from_slice(&this.decrypt_buffer[..decrypted_len]);

                        // 提供数据给调用者
                        let to_copy = std::cmp::min(this.read_buffer.len(), buf.remaining());
                        buf.put_slice(&this.read_buffer[..to_copy]);
                        this.read_buffer.advance(to_copy);

                        Poll::Ready(Ok(()))
                    }
                    Err(e) => Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Decryption error: {}", e),
                    ))),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for NoiseStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = &mut *self;

        // 加密数据
        match this.state.write_message(buf, &mut this.encrypt_buffer) {
            Ok(encrypted_len) => {
                // 写入加密后的数据
                match Pin::new(&mut this.inner)
                    .poll_write(cx, &this.encrypt_buffer[..encrypted_len])
                {
                    Poll::Ready(Ok(_)) => Poll::Ready(Ok(buf.len())),
                    Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                    Poll::Pending => Poll::Pending,
                }
            }
            Err(e) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Encryption error: {}", e),
            ))),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl NoiseClientConnector {
    /// 创建新的 Noise 客户端连接器
    pub fn new(
        static_key: Option<Vec<u8>>,
        remote_public_key: Option<Vec<u8>>,
    ) -> Result<Self, KvError> {
        let config = NoiseConfig::new(static_key, remote_public_key, true);

        Ok(Self {
            config: Arc::new(config),
        })
    }

    /// 触发 Noise 协议握手，把底层的 stream 转换成 Noise stream
    pub async fn connect<S>(&self, mut stream: S) -> Result<NoiseStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let mut builder = Builder::new(PROTOCOL_NAME.parse()?);

        if let Some(key) = &self.config.static_key {
            builder = builder.local_private_key(key)?;
        }

        if let Some(key) = &self.config.remote_public_key {
            builder = builder.remote_public_key(key)?;
        }

        let mut state = builder.build_initiator()?;
        let mut buffer = vec![0u8; 65536];

        // 握手阶段 1: 发送消息
        let len = state.write_message(&[], &mut buffer)?;
        tokio::io::AsyncWriteExt::write_all(&mut stream, &buffer[..len]).await?;

        // 握手阶段 2: 接收并处理响应
        let len = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer).await?;
        state.read_message(&buffer[..len], &mut vec![])?;

        // 握手阶段 3: 发送最终消息
        let len = state.write_message(&[], &mut buffer)?;
        tokio::io::AsyncWriteExt::write_all(&mut stream, &buffer[..len]).await?;

        let transport = state.into_transport_mode()?;

        Ok(NoiseStream::new(stream, transport))
    }
}

impl NoiseServerAcceptor {
    /// 创建新的 Noise 服务器接收器
    pub fn new(static_key: Option<Vec<u8>>) -> Result<Self, KvError> {
        let config = NoiseConfig::new(static_key, None, false);

        Ok(Self {
            config: Arc::new(config),
        })
    }

    /// 触发 Noise 协议握手，把底层的 stream 转换成 Noise stream
    pub async fn accept<S>(&self, mut stream: S) -> Result<NoiseStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let mut builder = Builder::new(PROTOCOL_NAME.parse()?);

        if let Some(key) = &self.config.static_key {
            builder = builder.local_private_key(key)?;
        }

        if let Some(key) = &self.config.remote_public_key {
            builder = builder.remote_public_key(key)?;
        }

        let mut state = builder.build_responder()?;
        let mut buffer = vec![0u8; 65536];

        // 握手阶段 1: 接收客户端消息
        let len = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer).await?;
        state.read_message(&buffer[..len], &mut vec![])?;

        // 握手阶段 2: 发送响应
        let len = state.write_message(&[], &mut buffer)?;
        tokio::io::AsyncWriteExt::write_all(&mut stream, &buffer[..len]).await?;

        // 握手阶段 3: 接收最终消息
        let len = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer).await?;
        state.read_message(&buffer[..len], &mut vec![])?;

        let transport = state.into_transport_mode()?;

        Ok(NoiseStream::new(stream, transport))
    }
}

/// 加载密钥文件
pub fn load_key(key: &str) -> Result<Vec<u8>, KvError> {
    // 尝试 base64 解码
    if let Ok(decoded) = base64::decode(key.trim()) {
        if decoded.len() == 32 {
            // X25519 密钥应该是32字节
            return Ok(decoded);
        }
    }

    // 如果不是base64或者是错误格式，返回错误
    Err(KvError::CertifcateParseError("noise", "key"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    // 测试用的密钥（使用 base64 编码）
    const SERVER_PRIVATE_KEY: &str = "JIxScvo9HTaq2XANzJ6qaN4D9yRFjrXU88eg+YORCu0=";
    const SERVER_PUBLIC_KEY: &str = "td93qlE0OqmfSyzxwkIMW2qDTbwDQZYSKqOdpgzPlQQ=";
    const CLIENT_PRIVATE_KEY: &str = "qRHG7HlaAQi+npHO+Wne6UegYI966bzgbUlA+1RlCBI=";
    const CLIENT_PUBLIC_KEY: &str = "87RyNtKl+piief594pgehOBYZ/YBx4qxIMhGJzGNGRg=";

    #[tokio::test]
    async fn noise_should_work() -> Result<()> {
        // 不使用静态密钥的简单测试
        let addr = start_server(None).await?;

        let connector = NoiseClientConnector::new(None, None)?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = connector.connect(stream).await?;
        stream.write_all(b"hello world!").await?;
        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");

        Ok(())
    }

    #[tokio::test]
    async fn noise_with_static_keys_should_work() -> Result<()> {
        // 使用静态密钥的测试
        let server_private_key = Some(load_key(SERVER_PRIVATE_KEY)?);
        let addr = start_server(server_private_key).await?;

        let client_private_key = Some(load_key(CLIENT_PRIVATE_KEY)?);
        let server_public_key = Some(load_key(SERVER_PUBLIC_KEY)?);

        let connector = NoiseClientConnector::new(client_private_key, server_public_key)?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = connector.connect(stream).await?;
        stream.write_all(b"hello world!").await?;
        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");

        Ok(())
    }

    #[tokio::test]
    async fn noise_with_mismatched_keys_should_fail() -> Result<()> {
        // 测试密钥不匹配的情况
        let server_private_key = Some(load_key(SERVER_PRIVATE_KEY)?);
        let addr = start_server(server_private_key).await?;

        // 客户端使用错误的服务器公钥
        let client_private_key = Some(load_key(CLIENT_PRIVATE_KEY)?);
        let wrong_public_key = Some(b"wrong_public_key_32_bytes_long_!".to_vec());

        let connector = NoiseClientConnector::new(client_private_key, wrong_public_key)?;
        let stream = TcpStream::connect(addr).await?;

        // 握手应该失败
        let result = connector.connect(stream).await;
        assert!(result.is_err());

        Ok(())
    }

    async fn start_server(static_key: Option<Vec<u8>>) -> Result<std::net::SocketAddr> {
        let acceptor = NoiseServerAcceptor::new(static_key)?;

        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = echo.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = echo.accept().await.unwrap();
            match acceptor.accept(stream).await {
                Ok(mut stream) => {
                    let mut buf = [0; 12];
                    if let Ok(_) = stream.read_exact(&mut buf).await {
                        let _ = stream.write_all(&buf).await;
                    }
                }
                Err(e) => {
                    eprintln!("Server accept failed: {:?}", e);
                }
            }
        });

        Ok(addr)
    }

    // 添加一个测试来验证密钥加载
    #[test]
    fn test_load_key() -> Result<()> {
        let key_data = load_key(SERVER_PRIVATE_KEY)?;
        assert_eq!(key_data.len(), 32); // X25519 密钥应该是32字节

        // 测试无效的base64
        let result = load_key("invalid_base64!!!");
        assert!(result.is_err());

        Ok(())
    }
}
