#[macro_use]
extern crate log;

use anyhow::Result;
use std::net::{TcpListener, TcpStream};

fn handle_client(stream: TcpStream) -> Result<()> {
    info!("Handling client: {}", stream.peer_addr()?);
    Ok(())
}

fn main() -> Result<()> {
    env_logger::init();

    let listener = TcpListener::bind("127.0.0.1:1935")?;

    info!("RTMP Relay listening on {}", listener.local_addr()?);

    for stream in listener.incoming() {
        handle_client(stream?)?;
    }

    Ok(())
}
