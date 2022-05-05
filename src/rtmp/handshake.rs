use anyhow::Result;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use std::io::prelude::*;
use std::net::TcpStream;

enum HandshakeStatus {
    InProgress(Vec<u8>),
    Completed(Vec<u8>),
}

pub fn handle_handshake(stream: &mut TcpStream) -> Result<()> {
    let mut handshake = Handshake::new(PeerType::Server);
    let mut buffer = [0u8; 1024];

    loop {
        let bytes_read = stream.read(&mut buffer)?;
        let handshake_result = handshake.process_bytes(&buffer[..bytes_read])?;
        match handle_handshake_result(handshake_result) {
            HandshakeStatus::InProgress(bytes) => {
                stream.write_all(&bytes)?;
            }
            HandshakeStatus::Completed(bytes) => {
                stream.write_all(&bytes)?;
                stream.flush()?;
                break;
            }
        }
    }

    Ok(())
}

fn handle_handshake_result(handshake_result: HandshakeProcessResult) -> HandshakeStatus {
    match handshake_result {
        HandshakeProcessResult::InProgress {
            response_bytes: bytes,
        } => {
            debug!("handshake in progress");
            HandshakeStatus::InProgress(bytes)
        }
        HandshakeProcessResult::Completed {
            response_bytes: bytes,
            remaining_bytes: _,
        } => {
            debug!("handshake completed");
            HandshakeStatus::Completed(bytes)
        }
    }
}
