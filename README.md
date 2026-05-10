# cloudcache

Prototype Linux userspace block device backed by a local persistent cache and a
local mock cloud object store.

The first backend is `local_mock`, which stores fixed-size cloud chunks in a
normal directory. The Linux device frontend uses NBD as the MVP kernel bridge;
the cache engine, journal, metadata, and sync logic are testable without a block
device.

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

