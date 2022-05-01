mod handshake;

#[macro_use]
extern crate log;

use crate::handshake::make_handshake;
use anyhow::Result;
use dotenv::dotenv;
use std::net::{TcpListener, TcpStream};

fn handle_client(mut stream: TcpStream) -> Result<()> {
    info!("Handling client: {}", stream.peer_addr()?);

    make_handshake(&mut stream)?;

    Ok(())
}

fn main() -> Result<()> {
    dotenv()?;
    env_logger::init();

    let listener = TcpListener::bind("127.0.0.1:1935")?;

    info!("RTMP Relay listening on {}", listener.local_addr()?);

    for stream in listener.incoming() {
        handle_client(stream?)?;
    }

    Ok(())
}
