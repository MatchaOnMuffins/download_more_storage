use crate::cache::engine::CacheEngine;
use crate::error::{CloudError, Result};
use libc::{
    AF_UNIX, POLLERR, POLLHUP, POLLIN, SOCK_STREAM, c_ulong, close, ioctl, poll, pollfd, socketpair,
};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;

const NBD_SET_SOCK: c_ulong = 0xab00;
const NBD_SET_BLKSIZE: c_ulong = 0xab01;
const NBD_DO_IT: c_ulong = 0xab03;
const NBD_CLEAR_SOCK: c_ulong = 0xab04;
const NBD_CLEAR_QUE: c_ulong = 0xab05;
const NBD_SET_SIZE_BLOCKS: c_ulong = 0xab07;
const NBD_DISCONNECT: c_ulong = 0xab08;
const NBD_SET_FLAGS: c_ulong = 0xab0a;

const NBD_FLAG_SEND_FLUSH: c_ulong = 1 << 2;
const NBD_FLAG_SEND_TRIM: c_ulong = 1 << 5;

const NBD_REQUEST_MAGIC: u32 = 0x2560_9513;
const NBD_REPLY_MAGIC: u32 = 0x6744_6698;

const NBD_CMD_READ: u32 = 0;
const NBD_CMD_WRITE: u32 = 1;
const NBD_CMD_DISC: u32 = 2;
const NBD_CMD_FLUSH: u32 = 3;
const NBD_CMD_TRIM: u32 = 4;
const NBD_CMD_WRITE_ZEROES: u32 = 6;

const EIO: u32 = 5;
const EINVAL: u32 = 22;
pub fn run_nbd(device_path: &str, mut engine: CacheEngine) -> Result<()> {
    let nbd = OpenOptions::new()
        .read(true)
        .write(true)
        .open(device_path)?;
    let mut sockets = [0 as libc::c_int; 2];
    let socketpair_rc = unsafe { socketpair(AF_UNIX, SOCK_STREAM, 0, sockets.as_mut_ptr()) };
    if socketpair_rc != 0 {
        return Err(CloudError::Io(std::io::Error::last_os_error()));
    }

    let nbd_fd = nbd.as_raw_fd();
    ioctl_ok(
        nbd_fd,
        NBD_SET_BLKSIZE,
        engine.cloud_manifest.sector_size_bytes,
    )?;
    ioctl_ok(
        nbd_fd,
        NBD_SET_SIZE_BLOCKS,
        engine.cloud_manifest.volume_size_bytes / engine.cloud_manifest.sector_size_bytes,
    )?;
    ioctl_ok(
        nbd_fd,
        NBD_SET_FLAGS,
        NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM,
    )?;
    ioctl_ok(nbd_fd, NBD_SET_SOCK, sockets[0] as c_ulong)?;

    unsafe {
        close(sockets[0]);
    }

    let nbd_done = Arc::new(AtomicBool::new(false));
    let nbd_done_thread = Arc::clone(&nbd_done);
    let handle = thread::spawn(move || {
        let rc = unsafe { ioctl(nbd.as_raw_fd(), NBD_DO_IT) };
        let _ = unsafe { ioctl(nbd.as_raw_fd(), NBD_CLEAR_QUE) };
        let _ = unsafe { ioctl(nbd.as_raw_fd(), NBD_CLEAR_SOCK) };
        nbd_done_thread.store(true, Ordering::Release);
        rc
    });

    let mut socket = unsafe { File::from_raw_fd(sockets[1] as RawFd) };
    let server_result = serve_requests(&mut socket, &mut engine, &nbd_done);
    drop(socket);
    let _ = handle.join();
    server_result
}

pub fn disconnect_nbd(device_path: &str) -> Result<()> {
    let nbd = OpenOptions::new()
        .read(true)
        .write(true)
        .open(device_path)?;
    ioctl_ok(nbd.as_raw_fd(), NBD_DISCONNECT, 0)?;
    println!("disconnected {device_path}");
    Ok(())
}

fn serve_requests(
    socket: &mut File,
    engine: &mut CacheEngine,
    nbd_done: &AtomicBool,
) -> Result<()> {
    loop {
        if !wait_for_request(socket.as_raw_fd(), nbd_done)? {
            break;
        }
        let mut header = [0u8; 28];
        match socket.read_exact(&mut header) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(CloudError::Io(err)),
        }

        let magic = u32::from_be_bytes(header[0..4].try_into().unwrap());
        if magic != NBD_REQUEST_MAGIC {
            return Err(CloudError::Corrupt(format!(
                "bad NBD request magic {magic:#x}"
            )));
        }
        let type_flags = u32::from_be_bytes(header[4..8].try_into().unwrap());
        let command = type_flags & 0x0000_ffff;
        let handle: [u8; 8] = header[8..16].try_into().unwrap();
        let offset = u64::from_be_bytes(header[16..24].try_into().unwrap());
        let length = u32::from_be_bytes(header[24..28].try_into().unwrap()) as usize;

        match command {
            NBD_CMD_READ => match engine.read_at(offset, length) {
                Ok(data) => {
                    write_reply(socket, 0, handle)?;
                    socket.write_all(&data)?;
                }
                Err(err) => {
                    eprintln!("cloudcache nbd read error: {err}");
                    write_reply(socket, EIO, handle)?;
                }
            },
            NBD_CMD_WRITE => {
                let mut data = vec![0u8; length];
                socket.read_exact(&mut data)?;
                match engine.write_at(offset, &data) {
                    Ok(()) => write_reply(socket, 0, handle)?,
                    Err(err) => {
                        eprintln!("cloudcache nbd write error: {err}");
                        write_reply(socket, EIO, handle)?;
                    }
                }
            }
            NBD_CMD_FLUSH => match engine.flush() {
                Ok(()) => write_reply(socket, 0, handle)?,
                Err(err) => {
                    eprintln!("cloudcache nbd flush error: {err}");
                    write_reply(socket, EIO, handle)?;
                }
            },
            NBD_CMD_DISC => {
                engine.flush()?;
                break;
            }
            NBD_CMD_TRIM => {
                write_reply(socket, 0, handle)?;
            }
            NBD_CMD_WRITE_ZEROES => {
                let zeroes = vec![0u8; length];
                match engine.write_at(offset, &zeroes) {
                    Ok(()) => write_reply(socket, 0, handle)?,
                    Err(err) => {
                        eprintln!("cloudcache nbd write_zeroes error: {err}");
                        write_reply(socket, EIO, handle)?;
                    }
                }
            }
            _ => {
                write_reply(socket, EINVAL, handle)?;
            }
        }
    }
    Ok(())
}

fn wait_for_request(fd: RawFd, nbd_done: &AtomicBool) -> Result<bool> {
    loop {
        if nbd_done.load(Ordering::Acquire) {
            return Ok(false);
        }
        let mut poll_fd = pollfd {
            fd,
            events: POLLIN,
            revents: 0,
        };
        let rc = unsafe { poll(&mut poll_fd, 1, 500) };
        if rc < 0 {
            return Err(CloudError::Io(std::io::Error::last_os_error()));
        }
        if rc == 0 {
            continue;
        }
        if poll_fd.revents & (POLLHUP | POLLERR) != 0 {
            return Ok(false);
        }
        if poll_fd.revents & POLLIN != 0 {
            return Ok(true);
        }
    }
}

fn write_reply(socket: &mut File, error: u32, handle: [u8; 8]) -> Result<()> {
    socket.write_all(&NBD_REPLY_MAGIC.to_be_bytes())?;
    socket.write_all(&error.to_be_bytes())?;
    socket.write_all(&handle)?;
    Ok(())
}

fn ioctl_ok(fd: RawFd, request: c_ulong, arg: c_ulong) -> Result<()> {
    let rc = unsafe { ioctl(fd, request, arg) };
    if rc == 0 {
        Ok(())
    } else {
        Err(CloudError::Io(std::io::Error::last_os_error()))
    }
}
