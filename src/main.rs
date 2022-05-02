mod handshake;

#[macro_use]
extern crate log;

use crate::handshake::make_handshake;
use anyhow::{anyhow, Result};
use dotenv::dotenv;
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

fn handle_client(mut stream: TcpStream) -> Result<()> {
    info!("Handling client: {}", stream.peer_addr()?);

    make_handshake(&mut stream)?;

    let (mut session, results) = ServerSession::new(ServerSessionConfig::new())?;

    for result in results {
        if let Some(event) = handle_result(result, &mut stream)? {
            debug!("handling event: {:#?}", event);
        }
    }

    let mut buffer = [0u8; 1024];

    loop {
        let bytes_read = stream.read(&mut buffer)?;
        let results = session.handle_input(&buffer[..bytes_read])?;
        for result in results {
            if let Some(event) = handle_result(result, &mut stream)? {
                match event {
                    ServerSessionEvent::ConnectionRequested {
                        request_id,
                        app_name,
                    } => {
                        info!(
                            "accepting connection request {} for app {}",
                            request_id, app_name
                        );
                        let results = session.accept_request(request_id)?;
                        for result in results {
                            if let Some(event) = handle_result(result, &mut stream)? {
                                debug!("handling event after accepting connection {:?}", event);
                            }
                        }
                    }
                    event => debug!("handling event: {:?}", event),
                };
            }
        }
    }
}

fn handle_result(
    result: ServerSessionResult,
    stream: &mut TcpStream,
) -> Result<Option<ServerSessionEvent>> {
    match result {
        ServerSessionResult::RaisedEvent(event) => {
            debug!("server session raised event: {:?}", event);
            Ok(Some(event))
        }
        ServerSessionResult::OutboundResponse(packet) => {
            trace!("server session outbound response: {:?}", packet);
            if !packet.can_be_dropped {
                stream.write_all(&packet.bytes)?;
                stream.flush()?;
            }
            Ok(None)
        }
        ServerSessionResult::UnhandleableMessageReceived(payload) => {
            error!("server session unhandled message received: {:?}", payload);
            Err(anyhow!("unhandled message"))
        }
    }
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
