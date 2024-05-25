use bytes::BytesMut;
use log::debug;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinSet;

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let _ = handle_connection(socket).await;
        });
    }
}

async fn handle_connection(down_socket: TcpStream) -> Result<()> {
    let up_socket = TcpStream::connect("127.0.0.1:8081").await?;

    let (down_sock_rx, down_sock_tx) = down_socket.into_split();
    let (up_sock_rx, up_sock_tx) = up_socket.into_split();

    let mut set = JoinSet::new();

    let (source, sink): (Sender<BytesMut>, Receiver<BytesMut>) = mpsc::channel(4);
    let (sink_return, source_return): (Sender<BytesMut>, Receiver<BytesMut>) = mpsc::channel(4);

    set.spawn(async move {
        read(down_sock_rx, source, source_return, "downstream").await;
    });
    set.spawn(async move {
        write(up_sock_tx, sink, sink_return, "upstream").await;
    });

    let (source, sink): (Sender<BytesMut>, Receiver<BytesMut>) = mpsc::channel(4);
    let (sink_return, source_return): (Sender<BytesMut>, Receiver<BytesMut>) = mpsc::channel(4);

    set.spawn(async move {
        read(up_sock_rx, source, source_return, "upstream").await;
    });
    set.spawn(async move {
        write(down_sock_tx, sink, sink_return, "downstream").await;
    });

    while (set.join_next().await).is_some() {}

    Ok(())
}

async fn read(
    mut sock_rx: OwnedReadHalf,
    source: Sender<BytesMut>,
    mut source_return: Receiver<BytesMut>,
    side: &str,
) {
    debug!("{side} reader: starting");
    let mut buf_count = 0;
    loop {
        let mut buf: BytesMut = if buf_count < 2 {
            debug!("{side} reader: creating new buffer");
            buf_count += 1;
            BytesMut::with_capacity(1024)
        } else {
            debug!("{side} reader: waiting for return buffer");
            match source_return.recv().await {
                Some(mut buf) => {
                    debug!("{side} reader: got return buffer");
                    buf.clear();
                    buf
                }
                None => {
                    debug!("{side} reader: no return buffer available");
                    break;
                }
            }
        };

        debug!("{side} reader: Waiting for data to read...");
        match sock_rx.read_buf(&mut buf).await {
            Ok(_) => {
                if buf.is_empty() {
                    debug!("{side} reader: read 0 bytes, closing");
                    break;
                }
                debug!(
                    "{side} reader: read {} bytes, passing buffer to other side",
                    buf.len()
                );
                if source.send(buf).await.is_err() {
                    debug!("{side} reader: error passing buffer to other side");
                    break;
                }
            }
            Err(_) => {
                debug!("{side} reader: read error");
                break;
            }
        }
    }
    debug!("{side} reader: exiting");
}

async fn write(
    mut sock_tx: OwnedWriteHalf,
    mut sink: Receiver<BytesMut>,
    sink_return: Sender<BytesMut>,
    side: &str,
) {
    debug!("{side} writer: starting and waiting for a buffer");
    while let Some(mut buf) = sink.recv().await {
        debug!("{side} writer: got a buffer, writing...");
        match sock_tx.write_all_buf(&mut buf).await {
            Ok(_) => {
                debug!("{side} writer: write complete, clearing buffer and sending back");
                buf.clear();
                if sink_return.send(buf).await.is_err() {
                    debug!("{side} writer: error returning buffer");
                    break;
                }
                debug!("{side} writer: buffer returned");
            }
            Err(_) => {
                debug!("{side} writer: write error");
                break;
            }
        }
        debug!("{side} writer: Waiting for another buffer");
    }
    debug!("{side} writer: exiting");
}
