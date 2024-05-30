# Testing

Here are some techniques to test the regulator locally.

## Upload testing

Prerequisites:
- Docker
- Regulator at `target/release/regulator`

```bash
# Start a web server that accepts uploads.
export tmp_dir=$(mktemp -d)
mkdir $tmp_dir/conf.d
cat << EOF > $tmp_dir/conf.d/default.conf
client_max_body_size 100g;
server {
    listen 80;
    location / {
        # Read the entire request body and discard it.
        echo_read_request_body;
        echo "Got it\n";
    }
}
EOF
docker run --name openresty -d -p 8080:80 -v $tmp_dir/conf.d:/etc/nginx/conf.d openresty/openresty

# Start the regulator.  Limit upload rate to 10 MB/sec
./target/release/regulator -l 127.0.0.1:9000 -c 127.0.0.1:8080 -u 10485760 2>regulator.log &

# Upload a 100 MB file
fallocate -l 100MiB bigfile
time curl -v -F "file=@bigfile" http://127.0.0.1:9000/

# Check regulator log
cat regulator.log

# Clean up
kill %1
docker stop openresty
docker rm openresty
rm -rf $tmp_dir
rm bigfile regulator.log
```