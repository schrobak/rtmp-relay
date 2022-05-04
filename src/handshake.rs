use anyhow::Result;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use std::io::prelude::*;
use std::net::TcpStream;

pub fn make_handshake(stream: &mut TcpStream) -> Result<()> {
    let mut handshake = Handshake::new(PeerType::Server);
    let mut buffer = [0u8; 1024];

    loop {
        let bytes_read = stream.read(&mut buffer)?;
        let (is_completed, response_bytes) = match handshake.process_bytes(&buffer[..bytes_read])? {
            HandshakeProcessResult::InProgress {
                response_bytes: bytes,
            } => (false, bytes),
            HandshakeProcessResult::Completed {
                response_bytes: bytes,
                remaining_bytes: _,
            } => {
                info!("handshake completed");
                (true, bytes)
            }
        };

        if !response_bytes.is_empty() {
            stream.write_all(&response_bytes)?;
        }

        if is_completed {
            stream.flush()?;
            break;
        }
    }

    Ok(())
}
