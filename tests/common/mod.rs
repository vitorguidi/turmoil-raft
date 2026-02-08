pub mod oracle;
pub use oracle::*;

pub mod linearizability_oracle;
pub use linearizability_oracle::*;

use hyper::Uri;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::transport::Endpoint;

pub mod incoming {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tonic::transport::server::{Connected, TcpConnectInfo};
    use turmoil::net::TcpStream;

    pub struct Accepted(pub TcpStream);

    impl Connected for Accepted {
        type ConnectInfo = TcpConnectInfo;

        fn connect_info(&self) -> Self::ConnectInfo {
            Self::ConnectInfo {
                local_addr: self.0.local_addr().ok(),
                remote_addr: self.0.peer_addr().ok(),
            }
        }
    }

    impl AsyncRead for Accepted {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for Accepted {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }
}

pub mod connector {
    use std::{future::Future, pin::Pin};

    use hyper::Uri;
    use hyper_util::rt::TokioIo;

    use tower::Service;
    use turmoil::net::TcpStream;

    type Fut = Pin<Box<dyn Future<Output = Result<TokioIo<TcpStream>, std::io::Error>> + Send>>;

    pub fn connector(
    ) -> impl Service<Uri, Response = TokioIo<TcpStream>, Error = std::io::Error, Future = Fut> + Clone
    {
        tower::service_fn(|uri: Uri| {
            Box::pin(async move {
                let conn = TcpStream::connect(uri.authority().unwrap().as_str()).await?;
                Ok::<_, std::io::Error>(TokioIo::new(conn))
            }) as Fut
        })
    }
}

pub fn create_channel(addr: &str) -> Channel {
    let uri = format!("http://{}", addr).parse::<Uri>().unwrap();
    Endpoint::from(uri)
        // Detect dead connections (e.g. after turmoil partition heals)
        .http2_keep_alive_interval(std::time::Duration::from_millis(500))
        .keep_alive_timeout(std::time::Duration::from_millis(1000))
        .connect_with_connector_lazy(connector::connector())
}

pub fn listener_stream(
    listener: turmoil::net::TcpListener,
) -> ReceiverStream<Result<incoming::Accepted, std::io::Error>> {
    let (tx, rx) = mpsc::channel(128);
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    tracing::info!("Server accepted connection");
                    if tx.send(Ok(incoming::Accepted(stream))).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("Server accept error: {:?}", e);
                    // Continue accepting connections even if one fails
                }
            }
        }
    });
    ReceiverStream::new(rx)
}