mod handshake;

#[macro_use]
extern crate log;

use crate::handshake::make_handshake;
use anyhow::{anyhow, Context, Result};
use dotenv::dotenv;
use env_logger::Env;
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use std::io::prelude::*;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::{io, thread, time};

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

    'client: loop {
        let bytes_read = stream.read(&mut buffer)?;
        let results = session.handle_input(&buffer[..bytes_read])?;
        for result in results {
            if let Some(event) = handle_result(result, &mut stream)? {
                match event {
                    ServerSessionEvent::ClientChunkSizeChanged { new_chunk_size } => {
                        debug!(
                            r#"new_chunk_size={} msg="{}""#,
                            new_chunk_size, "client chunk size changed"
                        );
                    }
                    ServerSessionEvent::ConnectionRequested {
                        request_id,
                        app_name,
                    } => {
                        info!(
                            r#"request_id={} app_name={} msg="{}""#,
                            request_id, app_name, "connection requested"
                        );
                        accept_session_request(request_id, &mut session, &mut stream)?;
                    }
                    ServerSessionEvent::ReleaseStreamRequested {
                        request_id,
                        app_name,
                        stream_key,
                    } => {
                        info!(
                            r#"request_id={} app_name={} stream_key={} msg="{}""#,
                            request_id, app_name, stream_key, "release stream requested"
                        );
                        accept_session_request(request_id, &mut session, &mut stream)?;
                    }
                    ServerSessionEvent::PublishStreamRequested {
                        request_id,
                        app_name,
                        stream_key,
                        mode,
                    } => {
                        info!(
                            r#"request_id={} app_name={} stream_key={} mode={:?} msg="{}""#,
                            request_id, app_name, stream_key, mode, "publish stream requested"
                        );
                        accept_session_request(request_id, &mut session, &mut stream)?;
                    }
                    ServerSessionEvent::PublishStreamFinished {
                        app_name,
                        stream_key,
                    } => {
                        info!(
                            r#"app_name={} stream_key={} msg="{}""#,
                            app_name, stream_key, "publish stream finished"
                        );
                        break 'client;
                    }
                    ServerSessionEvent::StreamMetadataChanged {
                        app_name,
                        stream_key,
                        metadata,
                    } => {
                        debug!(
                            r#"app_name={} stream_key={} msg="{}""#,
                            app_name, stream_key, "stream metadata changed"
                        );
                        trace!("{:#?}", metadata);
                    }
                    ServerSessionEvent::AudioDataReceived {
                        app_name,
                        stream_key,
                        data,
                        timestamp,
                    } => {
                        trace!(
                            r#"app_name={} stream_key={} data={} duration={} msg="{}""#,
                            app_name,
                            stream_key,
                            data.len(),
                            timestamp.value,
                            "audio data received"
                        );
                    }
                    ServerSessionEvent::VideoDataReceived {
                        app_name,
                        stream_key,
                        data,
                        timestamp,
                    } => {
                        trace!(
                            r#"app_name={} stream_key={} data={} duration={} msg="{}""#,
                            app_name,
                            stream_key,
                            data.len(),
                            timestamp.value,
                            "video data received"
                        );
                    }
                    ServerSessionEvent::UnhandleableAmf0Command {
                        transaction_id,
                        command_name,
                        command_object,
                        additional_values,
                    } => {
                        warn!(
                            r#"transaction_id={} command_name={} command_object={:?} additional_values={:?} msg="{}""#,
                            transaction_id,
                            command_name,
                            command_object,
                            additional_values,
                            "unhandled amf0 command"
                        );
                    }
                    ServerSessionEvent::PlayStreamRequested {
                        request_id,
                        app_name,
                        stream_key,
                        stream_id,
                        reset,
                        duration,
                        start_at,
                    } => {
                        info!(
                            r#"request_id={} app_name={} stream_key={} stream_id={} duration={:?} start_at={:?} reset={} msg="{}""#,
                            request_id,
                            app_name,
                            stream_key,
                            stream_id,
                            duration,
                            start_at,
                            reset,
                            "play stream requested"
                        );
                        accept_session_request(request_id, &mut session, &mut stream)?;
                    }
                    ServerSessionEvent::PlayStreamFinished {
                        app_name,
                        stream_key,
                    } => {
                        info!(
                            r#"app_name={} stream_key={} msg="{}""#,
                            app_name, stream_key, "play stream finished"
                        );
                    }
                    ServerSessionEvent::AcknowledgementReceived { bytes_received } => {
                        debug!(
                            r#"bytes_received={} msg="{}""#,
                            bytes_received, "ack received"
                        );
                    }
                    ServerSessionEvent::PingResponseReceived { timestamp } => {
                        debug!(r#"timestamp={:?} msg="{}""#, timestamp, "pong received");
                    }
                };
            }
        }
    }

    Ok(())
}

fn accept_session_request(
    request_id: u32,
    session: &mut ServerSession,
    stream: &mut TcpStream,
) -> Result<()> {
    let results = session.accept_request(request_id)?;
    for result in results {
        if let Some(event) = handle_result(result, stream)? {
            debug!("handling event after accepting request {:?}", event);
        }
    }
    Ok(())
}

fn handle_result(
    result: ServerSessionResult,
    stream: &mut TcpStream,
) -> Result<Option<ServerSessionEvent>> {
    match result {
        ServerSessionResult::RaisedEvent(event) => Ok(Some(event)),
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
    dotenv().ok();

    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let addr: SocketAddr = "127.0.0.1:1935"
        .parse()
        .context("Cannot parse RTMP socket address")?;
    let listener = TcpListener::bind(addr).context(format!("Cannot to bind {}", addr))?;
    listener
        .set_nonblocking(true)
        .context("set listener to non-blocking mode")?;

    info!("RTMP Relay listening on {}", addr);

    // FIXME: below code is terrible
    listener.incoming().for_each(|stream| match stream {
        Ok(stream) => match stream.set_nonblocking(false) {
            Ok(_) => match handle_client(stream) {
                Ok(_) => debug!("client done"),
                Err(err) => error!("cannot handle client\r\t{:#?}", err),
            },
            Err(err) => error!("cannot set stream to blocking mode\r\t{:#?}", err),
        },
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
            thread::sleep(time::Duration::from_millis(100));
        }
        Err(err) => error!("Cannot handle TcpStream because:\n\t{:#?}", err),
    });

    Ok(())
}
