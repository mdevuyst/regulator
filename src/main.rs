use bytes::{Buf, BytesMut};
use clap::Parser;
use log::{debug, info};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

type Error = Box<dyn std::error::Error>;
type Result<T> = std::result::Result<T, Error>;

/// Regulate the upload and download rates on a proxied TCP connection.
#[derive(Parser, Clone)]
#[command(version, about, long_about = None)]
struct Settings {
    /// Address to bind and listen on (given as host:port)
    #[arg(short, long)]
    listen: String,

    /// Upstream to proxy to (given as host:port)
    #[arg(short, long)]
    connect_to: String,

    /// The number of buffers to use for the upload
    #[arg(short = 'n', long, default_value_t = 8)]
    upload_num_bufs: usize,

    /// The number buffers to use for the download
    #[arg(short = 'm', long, default_value_t = 8)]
    download_num_bufs: usize,

    /// The size of each buffer for the upload
    #[arg(short = 's', long, default_value_t = 20 * 1014)]
    upload_buf_size: usize,

    /// The size of each buffer for the download
    #[arg(short = 't', long, default_value_t = 20 * 1024)]
    download_buf_size: usize,

    /// The upload rate (given in Bytes per second)
    #[arg(short, long)]
    upload_rate: Option<usize>,

    /// The download rate (given in Bytes per second)
    #[arg(short, long)]
    download_rate: Option<usize>,
}

#[derive(Debug)]
enum Side {
    Downstream,
    Upstream,
}

struct ReadOptions {
    sock_rx: OwnedReadHalf,
    source: Sender<Option<BytesMut>>,
    source_return: Receiver<BytesMut>,
    side: Side,
    session_number: u64,
    num_bufs: usize,
    buf_size: usize,
}

struct WriteOptions {
    sock_tx: OwnedWriteHalf,
    sink: Receiver<Option<BytesMut>>,
    sink_return: Sender<BytesMut>,
    side: Side,
    session_number: u64,
    rate: Option<usize>,
}

struct ReadStats {
    bytes_tranferred: usize,
    buffers_used: usize,
    smallest_buffer_length: usize,
    largest_buffer_length: usize,
    average_buffer_length: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let settings = Settings::parse();
    let listener = TcpListener::bind(&settings.listen).await?;
    let mut session_counter = 0;
    loop {
        let (socket, _) = listener.accept().await?;
        session_counter += 1;
        let settings_clone = settings.clone();
        tokio::spawn(async move {
            let res = handle_session(socket, session_counter, settings_clone).await;
            if let Err(e) = res {
                info!("Error: {:?}", e);
            }
        });
    }
}

async fn handle_session(
    down_socket: TcpStream,
    session_number: u64,
    settings: Settings,
) -> Result<()> {
    let now = std::time::Instant::now();
    let up_socket = TcpStream::connect(&settings.connect_to).await?;

    let (down_sock_rx, down_sock_tx) = down_socket.into_split();
    let (up_sock_rx, up_sock_tx) = up_socket.into_split();

    let (source, sink): (Sender<Option<BytesMut>>, Receiver<Option<BytesMut>>) =
        mpsc::channel(settings.upload_num_bufs);
    let (sink_return, source_return): (Sender<BytesMut>, Receiver<BytesMut>) =
        mpsc::channel(settings.upload_num_bufs);

    let read_opts = ReadOptions {
        sock_rx: down_sock_rx,
        source,
        source_return,
        side: Side::Downstream,
        session_number,
        num_bufs: settings.upload_num_bufs,
        buf_size: settings.upload_buf_size,
    };
    let downstream_rx = tokio::spawn(read(read_opts));

    let write_opts = WriteOptions {
        sock_tx: up_sock_tx,
        sink,
        sink_return,
        side: Side::Upstream,
        session_number,
        rate: settings.upload_rate,
    };
    let upstream_tx = tokio::spawn(write(write_opts));

    let (source, sink): (Sender<Option<BytesMut>>, Receiver<Option<BytesMut>>) =
        mpsc::channel(settings.download_num_bufs);
    let (sink_return, source_return): (Sender<BytesMut>, Receiver<BytesMut>) =
        mpsc::channel(settings.download_num_bufs);

    let read_opts = ReadOptions {
        sock_rx: up_sock_rx,
        source,
        source_return,
        side: Side::Upstream,
        session_number,
        num_bufs: settings.download_num_bufs,
        buf_size: settings.download_buf_size,
    };
    let upstream_rx = tokio::spawn(read(read_opts));

    let write_opts = WriteOptions {
        sock_tx: down_sock_tx,
        sink,
        sink_return,
        side: Side::Downstream,
        session_number,
        rate: settings.upload_rate,
    };
    let downstream_tx = tokio::spawn(write(write_opts));

    let downstream_read_stats = downstream_rx.await?;
    let upstream_read_stats = upstream_rx.await?;
    let downstream_bytes_written = downstream_tx.await?;
    let upstream_bytes_written = upstream_tx.await?;

    let elapsed = now.elapsed();

    info!(
        "[Session {}] Time: {:.6} sec, \
        Down RX: {} ({:.0} B/s) TX: {} ({:.0} B/s) \
        Up RX: {} ({:.0} B/s) TX: {} ({:.0} B/s)",
        session_number,
        elapsed.as_secs_f64(),
        downstream_read_stats.bytes_tranferred,
        downstream_read_stats.bytes_tranferred as f64 / elapsed.as_secs_f64(),
        downstream_bytes_written,
        downstream_bytes_written as f64 / elapsed.as_secs_f64(),
        upstream_read_stats.bytes_tranferred,
        upstream_read_stats.bytes_tranferred as f64 / elapsed.as_secs_f64(),
        upstream_bytes_written,
        upstream_bytes_written as f64 / elapsed.as_secs_f64(),
    );
    info!(
        "[Session {}] Upload bufs: {}, smallest: {}, largest: {}, average: {}",
        session_number,
        downstream_read_stats.buffers_used,
        downstream_read_stats.smallest_buffer_length,
        downstream_read_stats.largest_buffer_length,
        downstream_read_stats.average_buffer_length,
    );
    info!(
        "[Session {}] Download bufs: {}, smallest: {}, largest: {}, average: {}",
        session_number,
        upstream_read_stats.buffers_used,
        upstream_read_stats.smallest_buffer_length,
        upstream_read_stats.largest_buffer_length,
        upstream_read_stats.average_buffer_length,
    );

    Ok(())
}

async fn read(mut opts: ReadOptions) -> ReadStats {
    let session_number = opts.session_number;
    let side = opts.side;
    debug!("[Session {session_number}] {side:?} reader: starting");
    let mut buf_count = 0;
    let mut total_bufs_used = 0;
    let mut smallest_buffer_length = std::usize::MAX;
    let mut largest_buffer_length = 0;
    let mut total_bytes_read = 0;
    loop {
        let mut buf: BytesMut = if buf_count < opts.num_bufs {
            debug!(
                "[Session {session_number}] {side:?} reader: creating buffer {} of {}",
                buf_count + 1,
                opts.num_bufs
            );
            buf_count += 1;
            BytesMut::with_capacity(opts.buf_size)
        } else {
            debug!("[Session {session_number}] {side:?} reader: waiting for return buffer");
            match opts.source_return.recv().await {
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
        match opts.sock_rx.read_buf(&mut buf).await {
            Ok(_) => {
                total_bytes_read += buf.len();
                if buf.is_empty() {
                    // The peer has closed the connection.  Send a None to the
                    // writer to signal that it should exit.  Then wait for it to exit (by waiting
                    // for the mpsc channel to return an error, indicating that was closed).  If we
                    // exit immediately, the mpsc channel will be immediately closed and the writer
                    // may not have a chance to send all the buffers.
                    debug!("[Session {session_number}] {side:?} reader: read 0 bytes");
                    if opts.source.send(None).await.is_err() {
                        debug!(
                            "[Session {session_number}] {side:?} reader: error passing buffer to other side"
                        );
                        break;
                    }
                    // Wait until the writer exits.
                    while opts.source_return.recv().await.is_some() {}
                    debug!(
                        "[Session {session_number}] {side:?} reader: writer has exited, exiting"
                    );
                    break;
                }
                total_bufs_used += 1;
                smallest_buffer_length = std::cmp::min(smallest_buffer_length, buf.len());
                largest_buffer_length = std::cmp::max(largest_buffer_length, buf.len());
                debug!(
                    "[Session {session_number}] {side:?} reader: read {} bytes, passing buffer to other side",
                    buf.len()
                );
                if opts.source.send(Some(buf)).await.is_err() {
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
    ReadStats {
        bytes_tranferred: total_bytes_read,
        buffers_used: total_bufs_used,
        smallest_buffer_length,
        largest_buffer_length: if total_bufs_used > 0 {
            largest_buffer_length
        } else {
            0
        },
        average_buffer_length: if total_bufs_used != 0 {
            total_bytes_read / total_bufs_used
        } else {
            0
        },
    }
}

async fn write(mut opts: WriteOptions) -> usize {
    let session_number = opts.session_number;
    let side = opts.side;
    debug!("[Session {session_number}] {side:?} writer: starting and waiting for a buffer");
    let mut total_bytes_written = 0;
    while let Some(buf) = opts.sink.recv().await {
        // The reader will send a None when it has finished reading.  If we get a None, we should
        // exit (dropping the mpsc channel, and causing the reader to exit as well).
        let Some(mut buf) = buf else {
            debug!("[Session {session_number}] {side:?} writer: got None, exiting");
            break;
        };
        let buf_len = buf.len();
        debug!("[Session {session_number}] {side:?} writer: got a buffer, writing...");
        match opts.sock_tx.write_all_buf(&mut buf).await {
            Ok(_) => {
                total_bytes_written += buf_len;
                debug!("[Session {session_number}] {side:?} writer: write complete, clearing buffer and sending back");
                buf.clear();
                if opts.sink_return.send(buf).await.is_err() {
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
