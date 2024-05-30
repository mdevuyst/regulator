# regulator

A tool to proxy and rate limit TCP connections.

## Features

- Can limit both upload and download rates.
- Platform independent (tested on Linux and Windows)
- Can be used on client side or server side without the need to modify the client or server.
- Works with any protocol that runs over TCP.
- Supports multiple concurrent connections.
- Configurable buffer size and count.
- Logs stats at the end of every TCP session.

## Building

```bash
cargo build --release
```

The resulting binary will be `target/release/regulator` (or `target\release\regulator.exe` on Windows).

## Usage

At a minimum you must specify the address/port to bind to and the address/port
to proxy to.  You can also specify the maximum download and/or upload rate.

Run with `--help` to see the full set of options.

### Server side

Let's say you want to rate limit a web server to 2 KB/sec uploads and 4 KB/sec downloads.
You'll need to run the HTTP server on a different port, like 8080, and then run the regulator on the
port that clients will connect to (e.g., port 80):

```bash
./regulator --listen 0.0.0.0:80 --connect-to 127.0.0.1:8080 --upload-rate 2048 --download-rate 4096
```

or more concisely:

```bash
./regulator -l 0.0.0.0:80 -c 127.0.0.1:8080 -u 2048 -d 4096
```

### Client side

Let's say you want to upload a file with `curl` but limit the upload rate to 4 KB/sec.
Start the regulator like this:

```bash
./regulator -l 127.0.0.1:5000 -c upload.example.com:443 -u 4096
```

Then just add the argument `--connect-to ::127.0.0.1:5000` to the `curl` command.  E.g.:

```bash
curl -F "file=@bigfile.dat" --connect-to ::127.0.0.1:5000 https://upload.example.com/upload
```

Alternatively, you can modify the [hosts](https://en.wikipedia.org/wiki/Hosts_(file)) on your
system if you want to use a client that doesn't have something like curl's `--connect-to` argument.

## Architecture

When the `regulator` accepts a TCP connection on its listening socket, it establishes a new TCP
connection to the upstream server.  Then it splits both connections into receive and transmit
sides, which are managed by four concurrent tasks ([tokio](https://tokio.rs/) green threads).  So there are two
receivers and two transmitters per session for full duplex flow.


```plain
                  ┌─────────Session────────┐                
                  │                        │                
                  │  ┌───────┐  ┌───────┐  │                
                  │  │ Down  │  │  Up   │  │                
                  │  │ Sock  │  │  Sock │  │                
                  │  │ ┌──┐  │  │  ┌──┐ │  │                
                  │  │ │Rx├──┼──┼─►│Tx│ │  │                
┌────────┐        │  │ └──┘  │  │  └──┘ │  │      ┌────────┐
│ Client │◄───────┼─►│       │  │       │◄─┼─────►│ Server │
└────────┘        │  │ ┌──┐  │  │  ┌──┐ │  │      └────────┘
                  │  │ │Tx│◄─┼──┼──┤Rx│ │  │                
                  │  │ └──┘  │  │  └──┘ │  │                
                  │  │       │  │       │  │                
                  │  └───────┘  └───────┘  │                
                  │                        │                
                  └────────────────────────┘                
```

The downstream receive side and the upstream transmit side use a pair of channels for sharing buffers.
And the upstream receive and downstream transmit sides use another pair of channels.
Within each channel pair, one channel is used to pass full buffers of data from the receive side
to the transmit side and the other channel is a return channel to pass back empty buffers from the
transmit side back to the receive side to be reused.

Each receiver creates new buffers (as needed) of a configured size up to a configured maximum.
As soon as some data is read into a buffer, it's passed to the associated transmitter for transmission out
the other end.  Once sent, the transmitter returns the buffer to the receiver for reuse.
Once a receiver reaches its new buffer limit, it may have to wait for its transmitter to return back
a buffer before it can read more data from the socket.  This creates back pressure
to match the speed of the remote sender to the speed of the remote receiver.  It also prevents
runaway memory usage in the regulator process.

If a rate limit is configured, it is applied at the transmitter.  The algorithm tries to achieve the
target *average* transfer rate over the course of the session.  This may mean that some portions
of the transfer are a little slower or faster than the target rate (e.g., if network conditions
cause the transfer to slow below the desired rate for a few seconds, the transfer rate may then
go higher than the target rate for a few seconds to catch up so that the *average* rate is as
desired).  Short "sleep" periods between buffer transmissions are used to limit the send rate.
These are non-blocking (just as all the I/O operations are too) so they don't affect flow in the
other direction or other concurrent sessions.

The receivers and transmitters collect stat, and when a session completes, the stats are reported,
including the session time, number of bytes transferred in each direction, average transfer rate,
buffer utilization, etc.
