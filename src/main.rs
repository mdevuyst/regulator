use bytes::{Buf, BytesMut};
use bytesize::ByteSize;
use clap::Parser;
use log::{debug, error, info, warn};
use std::cmp::max;
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
    upload_num_bufs: u64,

    /// The number buffers to use for the download
    #[arg(short = 'm', long, default_value_t = 8)]
    download_num_bufs: u64,

    /// The size of each buffer for the upload
    #[arg(short = 's', long, default_value_t = 20 * 1014)]
    upload_buf_size: usize,

    /// The size of each buffer for the download
    #[arg(short = 't', long, default_value_t = 20 * 1024)]
    download_buf_size: usize,

    /// The upload rate (given in Bytes per second)
    #[arg(short, long)]
    upload_rate: Option<u64>,

    /// The download rate (given in Bytes per second)
    #[arg(short, long)]
    download_rate: Option<u64>,
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
    num_bufs: u64,
    buf_size: usize,
}

struct WriteOptions {
    sock_tx: OwnedWriteHalf,
    sink: Receiver<Option<BytesMut>>,
    sink_return: Sender<BytesMut>,
    side: Side,
    session_number: u64,
    rate: Option<u64>,
}

struct ReadStats {
    bytes_read: u64,
    buffers_used: u64,
    smallest_buffer_length: u64,
    largest_buffer_length: u64,
    average_buffer_length: u64,
}

struct WriteStats {
    bytes_written: u64,
    total_sleep_time: std::time::Duration,
    num_sleeps: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let settings = Settings::parse();
    let listener = TcpListener::bind(&settings.listen).await?;
    let mut session_counter = 0;
    info!("Listening on {}", settings.listen);
    loop {
        let (socket, _) = listener.accept().await?;
        session_counter += 1;
        info!(
            "[Session {session_counter}] Accepted new connection from {:?}",
            socket.peer_addr()?,
        );
        let settings_clone = settings.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_session(socket, session_counter, settings_clone).await {
                error!("[Session {session_counter}] {:?}", e);
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
    info!("{pre} Connected to {}", settings.connect_to);

    let (down_sock_rx, down_sock_tx) = down_socket.into_split();
    let (up_sock_rx, up_sock_tx) = up_socket.into_split();

    // Create channels to pass buffers between the downstream reader to the upstream writer (for uploads).
    // Then spawn the downstream reader and upstream writer.

    let (source, sink): (Sender<Option<BytesMut>>, Receiver<Option<BytesMut>>) =
        mpsc::channel(settings.upload_num_bufs as usize);
    let (sink_return, source_return): (Sender<BytesMut>, Receiver<BytesMut>) =
        mpsc::channel(settings.upload_num_bufs as usize);

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
        mpsc::channel(settings.download_num_bufs as usize);
    let (sink_return, source_return): (Sender<BytesMut>, Receiver<BytesMut>) =
        mpsc::channel(settings.download_num_bufs as usize);

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
    let downstream_write_stats = downstream_tx.await?;
    let upstream_write_stats = upstream_tx.await?;

    let elapsed = now.elapsed();

    if downstream_read_stats.bytes_read != upstream_write_stats.bytes_written {
        warn!(
            "[Session {}] Warning: Downstream read {} bytes, upstream wrote {} bytes",
            session_number, downstream_read_stats.bytes_read, upstream_write_stats.bytes_written,
        );
    }
    if upstream_read_stats.bytes_read != downstream_write_stats.bytes_written {
        warn!(
            "[Session {}] Warning: Upstream read {} bytes, downstream wrote {} bytes",
            session_number, upstream_read_stats.bytes_read, downstream_write_stats.bytes_written,
        );
    }

    info!(
        "[Session {}] Time: {:.3} sec, \
        Down RX: {} ({}/s) TX: {} ({}/s) \
        Up RX: {} ({}/s) TX: {} ({}/s)",
        session_number,
        elapsed.as_secs_f64(),
        ByteSize(downstream_read_stats.bytes_read),
        ByteSize(downstream_read_stats.bytes_read / max(elapsed.as_secs(), 1)),
        ByteSize(downstream_write_stats.bytes_written),
        ByteSize(downstream_write_stats.bytes_written / max(elapsed.as_secs(), 1)),
        ByteSize(upstream_read_stats.bytes_read),
        ByteSize(upstream_read_stats.bytes_read / max(elapsed.as_secs(), 1)),
        ByteSize(upstream_write_stats.bytes_written),
        ByteSize(upstream_write_stats.bytes_written / max(elapsed.as_secs(), 1)),
    );
    info!(
        "[Session {}] Upload bufs: {}, smallest: {}, largest: {}, average: {}",
        session_number,
        downstream_read_stats.buffers_used,
        ByteSize(downstream_read_stats.smallest_buffer_length),
        ByteSize(downstream_read_stats.largest_buffer_length),
        ByteSize(downstream_read_stats.average_buffer_length),
    );
    info!(
        "[Session {}] Download bufs: {}, smallest: {}, largest: {}, average: {}",
        session_number,
        upstream_read_stats.buffers_used,
        ByteSize(upstream_read_stats.smallest_buffer_length),
        ByteSize(upstream_read_stats.largest_buffer_length),
        ByteSize(upstream_read_stats.average_buffer_length),
    );
    info!(
        "[Session {}] Sleeps: Upload: {} ({:.3} sec), Download: {} ({:.3} sec)",
        session_number,
        upstream_write_stats.num_sleeps,
        upstream_write_stats.total_sleep_time.as_secs_f64(),
        downstream_write_stats.num_sleeps,
        downstream_write_stats.total_sleep_time.as_secs_f64(),
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
    let mut smallest_buffer_length = std::u64::MAX;
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
                    error!("{pre} no return buffer available");
                    break;
                }
            }
        };

        debug!("{pre} waiting for data to read...");
        match opts.sock_rx.read_buf(&mut buf).await {
            Ok(_) => {
                total_bytes_read += buf.len() as u64;
                if buf.is_empty() {
                    // The peer has closed the connection.  Send a None value to the
                    // writer to signal that it should exit.  Then wait for the writer to exit (by
                    // reading from the mpsc channel until it returns an error).  We should not just
                    // exit immediately because the mpsc channel will be immediately closed and the writer
                    // may not have a chance to send all the buffers.
                    debug!("{pre} read 0 bytes, notifying the writer and waiting...");
                    if opts.source.send(None).await.is_err() {
                        error!("{pre} error passing buf to other side");
                        break;
                    }
                    // Wait until the writer exits.
                    while opts.source_return.recv().await.is_some() {}
                    debug!("{pre} notified that the writer has exited");
                    break;
                }
                total_bufs_used += 1;
                smallest_buffer_length = std::cmp::min(smallest_buffer_length, buf.len() as u64);
                largest_buffer_length = std::cmp::max(largest_buffer_length, buf.len() as u64);
                debug!("{pre} read {} bytes, passing buf to other side", buf.len());
                if opts.source.send(Some(buf)).await.is_err() {
                    error!("{pre} error passing buf to other side");
                    break;
                }
            }
            Err(e) => {
                error!("{pre} read error: {:?}", e);
                break;
            }
        }
    }
    debug!("{pre} exiting");
    ReadStats {
        bytes_read: total_bytes_read,
        buffers_used: total_bufs_used,
        smallest_buffer_length: if total_bufs_used > 0 {
            smallest_buffer_length
        } else {
            0
        },
        largest_buffer_length,
        average_buffer_length: if total_bufs_used != 0 {
            total_bytes_read / total_bufs_used
        } else {
            0
        },
    }
}

async fn write(mut opts: WriteOptions) -> WriteStats {
    let start = std::time::Instant::now();
    let mut total_sleep_time = std::time::Duration::new(0, 0);
    let mut num_sleeps = 0;
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
        debug!("{pre} got a buffer of length {buf_len}");

        // If we have a rate limit, sleep for the appropriate amount of time.
        if let Some(rate) = opts.rate {
            let elapsed = start.elapsed();
            let expected_bytes = elapsed.as_secs_f64() * rate as f64;
            if total_bytes_written as f64 > expected_bytes {
                let overrun = total_bytes_written as f64 - expected_bytes;
                let sleep_time = std::time::Duration::from_secs_f64(overrun / rate as f64);
                total_sleep_time += sleep_time;
                num_sleeps += 1;
                debug!("{pre} sleeping for {:.6} seconds", sleep_time.as_secs_f64());
                tokio::time::sleep(sleep_time).await;
            }
        }

        debug!("{pre} writing {buf_len} bytes...");
        match opts.sock_tx.write_all_buf(&mut buf).await {
            Ok(_) => {
                total_bytes_written += buf_len as u64;
                debug!("{pre} write complete, clearing buffer and sending back");
                buf.clear();
                if opts.sink_return.send(buf).await.is_err() {
                    error!("{pre} error returning buffer");
                    break;
                }
                debug!("{pre} buffer returned");
            }
            Err(e) => {
                total_bytes_written += buf.remaining() as u64;
                error!("{pre} write error: {:?}", e);
                break;
            }
        }
        debug!("{pre} waiting for another buffer");
    }
    debug!("{pre} exiting");
    WriteStats {
        bytes_written: total_bytes_written,
        total_sleep_time,
        num_sleeps,
    }
}
