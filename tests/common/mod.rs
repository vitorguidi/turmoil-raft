use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use std::pin::Pin;
use std::task::{Context, Poll};

pub async fn grpc_connector(
    uri: Uri,
) -> Result<TokioIo<turmoil::net::TcpStream>, std::io::Error>
{
    let host = uri.host().unwrap_or("127.0.0.1").to_string();
    let port = uri.port_u16().unwrap_or(80);
    let stream = turmoil::net::TcpStream::connect((host.as_str(), port)).await?;
    tracing::info!("Client connected to {}:{}", host, port);
    Ok(TokioIo::new(stream))
}

pub fn create_channel(addr: &str) -> Channel {
    let uri = format!("http://{}", addr).parse::<Uri>().unwrap();
    Endpoint::from(uri).connect_with_connector_lazy(service_fn(grpc_connector))
}

#[derive(Debug)]
pub struct TurmoilConnection(pub turmoil::net::TcpStream);

impl tonic::transport::server::Connected for TurmoilConnection {
    type ConnectInfo = ();
    fn connect_info(&self) -> Self::ConnectInfo { () }
}

impl AsyncRead for TurmoilConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for TurmoilConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

pub fn listener_stream(
    listener: turmoil::net::TcpListener,
) -> ReceiverStream<Result<TurmoilConnection, std::io::Error>> {
    let (tx, rx) = mpsc::channel(128);
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    tracing::info!("Server accepted connection");
                    if tx.send(Ok(TurmoilConnection(stream))).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("Server accept error: {:?}", e);
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        }
    });
    ReceiverStream::new(rx)
}