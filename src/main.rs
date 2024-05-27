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
    debug!("Listening on {}", settings.listen);
    loop {
        let (socket, _) = listener.accept().await?;
        session_counter += 1;
        debug!("Accepted connection. Starting session {}", session_counter);
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
    let pre = format!("[Session {session_number}] ");
    debug!("{pre} Connecting to {}", settings.connect_to);
    let up_socket = TcpStream::connect(&settings.connect_to).await?;
    debug!("{pre} Connected to {}", settings.connect_to);

    let (down_sock_rx, down_sock_tx) = down_socket.into_split();
    let (up_sock_rx, up_sock_tx) = up_socket.into_split();

    // Create channels to pass buffers between the downstream reader to the upstream writer (for uploads).
    // Then spawn the downstream reader and upstream writer.

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

    // Create channels to pass buffers between the upstream reader to the downstream writer (for downloads).
    // Then spawn the upstream reader and downstream writer.

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

    // Wait for all the tasks to complete and then print the session stats.

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
    let pre = format!("[Session {session_number}] {side:?} reader:");
    debug!("{pre} starting");
    let mut buf_count = 0;
    let mut total_bufs_used = 0;
    let mut smallest_buffer_length = std::usize::MAX;
    let mut largest_buffer_length = 0;
    let mut total_bytes_read = 0;
    loop {
        // Get a buffer to read into.  If we have not reached the maximum number of buffers, create
        // a new one.  Otherwise, wait for a buffer to be returned from the writer.
        let mut buf: BytesMut = if buf_count < opts.num_bufs {
            debug!("{pre} creating buf {} of {}", buf_count + 1, opts.num_bufs);
            buf_count += 1;
            BytesMut::with_capacity(opts.buf_size)
        } else {
            debug!("{pre} waiting for return buffer");
            match opts.source_return.recv().await {
                Some(mut buf) => {
                    debug!("{pre} got return buffer");
                    buf.clear();
                    buf
                }
                None => {
                    debug!("{pre} no return buffer available");
                    break;
                }
            }
        };

        debug!("{pre} waiting for data to read...");
        match opts.sock_rx.read_buf(&mut buf).await {
            Ok(_) => {
                total_bytes_read += buf.len();
                if buf.is_empty() {
                    // The peer has closed the connection.  Send a None value to the
                    // writer to signal that it should exit.  Then wait for the writer to exit (by
                    // reading from the mpsc channel until it returns an error).  We should not just
                    // exit immediately because the mpsc channel will be immediately closed and the writer
                    // may not have a chance to send all the buffers.
                    debug!("{pre} read 0 bytes, notifying the writer and waiting...");
                    if opts.source.send(None).await.is_err() {
                        debug!("{pre} error passing buf to other side");
                        break;
                    }
                    // Wait until the writer exits.
                    while opts.source_return.recv().await.is_some() {}
                    debug!("{pre} notified that the writer has exited");
                    break;
                }
                total_bufs_used += 1;
                smallest_buffer_length = std::cmp::min(smallest_buffer_length, buf.len());
                largest_buffer_length = std::cmp::max(largest_buffer_length, buf.len());
                debug!("{pre} read {} bytes, passing buf to other side", buf.len());
                if opts.source.send(Some(buf)).await.is_err() {
                    debug!("{pre} error passing buf to other side");
                    break;
                }
            }
            Err(_) => {
                debug!("{pre} read error");
                break;
            }
        }
    }
    debug!("{pre} exiting");
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
    let pre = format!("[Session {session_number}] {side:?} writer:");
    debug!("{pre} starting and waiting for a buffer");
    let mut total_bytes_written = 0;
    while let Some(buf) = opts.sink.recv().await {
        // The reader will send a None when it has finished reading.  If we get a None, we should
        // exit (dropping the mpsc channel, and causing the reader to exit as well).
        let Some(mut buf) = buf else {
            debug!("{pre} got notification that the reader has finished");
            break;
        };
        let buf_len = buf.len();
        debug!("{pre} got a buffer of length {buf_len}, writing...");
        match opts.sock_tx.write_all_buf(&mut buf).await {
            Ok(_) => {
                total_bytes_written += buf_len;
                debug!("{pre} write complete, clearing buffer and sending back");
                buf.clear();
                if opts.sink_return.send(buf).await.is_err() {
                    debug!("{pre} error returning buffer");
                    break;
                }
                debug!("{pre} buffer returned");
            }
            Err(_) => {
                total_bytes_written += buf.remaining();
                debug!("{pre} write error");
                break;
            }
        }
        debug!("{pre} waiting for another buffer");
    }
    debug!("{pre} exiting");
    total_bytes_written
}
