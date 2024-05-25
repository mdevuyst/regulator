use bytes::{Buf, BytesMut};
use log::{debug, info};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
enum Side {
    Downstream,
    Upstream,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    let mut session_counter = 0;
    loop {
        let (socket, _) = listener.accept().await?;
        session_counter += 1;
        tokio::spawn(async move {
            let _ = handle_session(socket, session_counter).await;
        });
    }
}

async fn handle_session(down_socket: TcpStream, session_number: u64) -> Result<()> {
    let up_socket = TcpStream::connect("127.0.0.1:8081").await?;

    let (down_sock_rx, down_sock_tx) = down_socket.into_split();
    let (up_sock_rx, up_sock_tx) = up_socket.into_split();

    let (source, sink): (Sender<BytesMut>, Receiver<BytesMut>) = mpsc::channel(4);
    let (sink_return, source_return): (Sender<BytesMut>, Receiver<BytesMut>) = mpsc::channel(4);

    let downstream_rx = tokio::spawn(read(
        down_sock_rx,
        source,
        source_return,
        Side::Downstream,
        session_number,
    ));
    let upstream_tx = tokio::spawn(write(
        up_sock_tx,
        sink,
        sink_return,
        Side::Upstream,
        session_number,
    ));

    let (source, sink): (Sender<BytesMut>, Receiver<BytesMut>) = mpsc::channel(4);
    let (sink_return, source_return): (Sender<BytesMut>, Receiver<BytesMut>) = mpsc::channel(4);

    let upstream_rx = tokio::spawn(read(
        up_sock_rx,
        source,
        source_return,
        Side::Upstream,
        session_number,
    ));
    let downstream_tx = tokio::spawn(write(
        down_sock_tx,
        sink,
        sink_return,
        Side::Downstream,
        session_number,
    ));

    let downstream_bytes_read = downstream_rx.await?;
    let upstream_bytes_read = upstream_rx.await?;
    let downstream_bytes_written = downstream_tx.await?;
    let upstream_bytes_written = upstream_tx.await?;

    // TODO: report the duration of the flow.
    info!(
        "[Session {}] Downstream RX: {} TX: {} Upstream RX: {} TX: {}",
        session_number,
        downstream_bytes_read,
        downstream_bytes_written,
        upstream_bytes_read,
        upstream_bytes_written
    );

    Ok(())
}

async fn read(
    mut sock_rx: OwnedReadHalf,
    source: Sender<BytesMut>,
    mut source_return: Receiver<BytesMut>,
    side: Side,
    session_number: u64,
) -> usize {
    debug!("[Session {session_number}] {side:?} reader: starting");
    let mut buf_count = 0;
    let mut total_bytes_read = 0;
    loop {
        let mut buf: BytesMut = if buf_count < 2 {
            debug!("[Session {session_number}] {side:?} reader: creating new buffer");
            buf_count += 1;
            BytesMut::with_capacity(1024)
        } else {
            debug!("[Session {session_number}] {side:?} reader: waiting for return buffer");
            match source_return.recv().await {
                Some(mut buf) => {
                    debug!("[Session {session_number}] {side:?} reader: got return buffer");
                    buf.clear();
                    buf
                }
                None => {
                    debug!(
                        "[Session {session_number}] {side:?} reader: no return buffer available"
                    );
                    break;
                }
            }
        };

        debug!("[Session {session_number}] {side:?} reader: Waiting for data to read...");
        match sock_rx.read_buf(&mut buf).await {
            Ok(_) => {
                total_bytes_read += buf.len();
                if buf.is_empty() {
                    debug!("[Session {session_number}] {side:?} reader: read 0 bytes, closing");
                    break;
                }
                debug!(
                    "[Session {session_number}] {side:?} reader: read {} bytes, passing buffer to other side",
                    buf.len()
                );
                if source.send(buf).await.is_err() {
                    debug!(
                        "[Session {session_number}] {side:?} reader: error passing buffer to other side"
                    );
                    break;
                }
            }
            Err(_) => {
                debug!("[Session {session_number}] {side:?} reader: read error");
                break;
            }
        }
    }
    debug!("[Session {session_number}] {side:?} reader: exiting");
    total_bytes_read
}

async fn write(
    mut sock_tx: OwnedWriteHalf,
    mut sink: Receiver<BytesMut>,
    sink_return: Sender<BytesMut>,
    side: Side,
    session_number: u64,
) -> usize {
    debug!("[Session {session_number}] {side:?} writer: starting and waiting for a buffer");
    let mut total_bytes_written = 0;
    while let Some(mut buf) = sink.recv().await {
        let buf_len = buf.len();
        debug!("[Session {session_number}] {side:?} writer: got a buffer, writing...");
        match sock_tx.write_all_buf(&mut buf).await {
            Ok(_) => {
                total_bytes_written += buf_len;
                debug!("[Session {session_number}] {side:?} writer: write complete, clearing buffer and sending back");
                buf.clear();
                if sink_return.send(buf).await.is_err() {
                    debug!("[Session {session_number}] {side:?} writer: error returning buffer");
                    break;
                }
                debug!("[Session {session_number}] {side:?} writer: buffer returned");
            }
            Err(_) => {
                total_bytes_written += buf.remaining();
                debug!("[Session {session_number}] {side:?} writer: write error");
                break;
            }
        }
        debug!("[Session {session_number}] {side:?} writer: Waiting for another buffer");
    }
    debug!("[Session {session_number}] {side:?} writer: exiting");
    total_bytes_written
}
