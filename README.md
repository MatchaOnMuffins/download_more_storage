# cloudcache

Prototype Linux userspace block device backed by a local persistent cache and a
local mock cloud object store.

The first backend is `local_mock`, which stores fixed-size cloud chunks in a
normal directory. The Linux device frontend uses NBD as the MVP kernel bridge;
the cache engine, journal, metadata, and sync logic are testable without a block
device.

## Distribution status

This is still prototype storage software. It is appropriate for local testing,
VM smoke tests, and development of the cache/device architecture. It is not yet
ready for irreplaceable data.

Current constraints:

- Linux is required for `attach`/`detach`; non-Linux hosts can still run engine
  tests and metadata operations.
- NBD is the only device frontend.
- `local_mock` is the default backend. An experimental S3 backend is available
  behind the `s3` Cargo feature.
- Flush durability is local cache + journal durability. Cloud durability requires
  an explicit `cloudcache sync <volume>`.
- Only one writer should operate on a volume at a time.
- Unmount filesystems before detach or daemon shutdown.

## MVP semantics

- `READ` lazily fetches missing chunks from the mock cloud or returns zeroes for
  never-written chunks.
- `WRITE` is write-back: data is committed to the local chunk cache and journal
  before success is reported.
- `FLUSH` means all acknowledged writes are durable in the local cache and WAL.
- `cloudcache sync <volume>` uploads dirty chunks to the mock cloud.
- Clean chunks may be evicted; dirty/uploading/fetching chunks are never evicted.

Cloud durability and local durability are intentionally separate:

- after `WRITE + FLUSH`, data survives daemon restart and local reboot if the
  cache disk survives;
- after `cloudcache sync`, dirty data also exists in the backing object store.

## Quick local test

```sh
cargo test
cargo run -- create --volume-id test --size 1G --chunk-size 16M --cache-dir /tmp/cloudcache-test --remote-dir /tmp/cloudcache-remote
cargo run -- status test
cargo run -- sync test
```

If `--cache-dir` is omitted, cloudcache stores volumes under
`$CLOUDCACHE_HOME/volumes/<volume-id>` or `$HOME/.cloudcache/volumes/<volume-id>`.
The volume registry is `$CLOUDCACHE_REGISTRY` when set, otherwise
`$CLOUDCACHE_HOME/volumes.tsv` or `$HOME/.cloudcache/volumes.tsv`.

## S3 backend

Build with the S3 feature to include the AWS SDK:

```sh
cargo build --release --features s3
```

Create a volume backed by S3:

```sh
target/release/cloudcache create \
  --volume-id test-s3 \
  --size 1G \
  --chunk-size 16M \
  --cache-size 200G \
  --cloud-provider s3 \
  --bucket my-cloudcache-bucket \
  --prefix cloudcache/volumes/test-s3 \
  --region us-east-1
```

Credentials use the AWS SDK default provider chain. For S3-compatible local
testing, pass `--endpoint-url`, for example with LocalStack or MinIO. The object
layout is:

```text
<prefix>/manifest.json
<prefix>/chunks/<16-hex-digit-chunk-id>.chunk
```

The S3 backend validates object size on fetch and stores a `cloudcache-sha256`
metadata value on upload. This is still single-writer storage; do not attach the
same volume from multiple machines at once.

Current S3 test bucket:

```text
disk-swap-970508865888-us-east-1-an
```

Keep AWS credentials out of git. For local testing, export them in your shell or
put them in a local `.env` file; `.env` files are ignored by this repo:

```sh
export AWS_ACCESS_KEY_ID='...'
export AWS_SECRET_ACCESS_KEY='...'
export AWS_REGION='us-east-1'
```

## Linux NBD attach

On Linux with the `nbd` kernel module loaded:

```sh
sudo modprobe nbd max_part=8
sudo target/debug/cloudcache attach test --device /dev/nbd0
sudo mkfs.ext4 /dev/nbd0
sudo mount /dev/nbd0 /mnt/test
```

Detach from another terminal:

```sh
sudo target/debug/cloudcache detach test
```

This NBD server runs in the foreground and exits after disconnect.
