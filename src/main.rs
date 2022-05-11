mod rtmp;

#[macro_use]
extern crate log;
extern crate core;

use crate::rtmp::handshake;
use anyhow::{anyhow, Context, Result};
use derive_more::{Display, Error};
use dotenv::dotenv;
use env_logger::Env;
use gst_app::prelude::*;
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use std::io::prelude::*;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;
use std::{io, thread, time};

#[derive(Debug, Display, Error)]
#[display(fmt = "Received error from {}: {} (debug: {:?})", src, error, debug)]
struct ErrorMessage {
    src: String,
    error: String,
    debug: Option<String>,
    source: gst_app::glib::Error,
}

fn run_bus(pipeline: gst::Pipeline) -> Result<()> {
    pipeline.set_state(gst::State::Playing)?;

    let bus = pipeline
        .bus()
        .expect("Pipeline without bus. Shouldn't happen!");

    for msg in bus.iter_timed(gst::ClockTime::NONE) {
        use gst::MessageView;

        match msg.view() {
            MessageView::Eos(..) => break,
            MessageView::Error(err) => {
                pipeline.set_state(gst::State::Null)?;
                return Err(ErrorMessage {
                    src: msg
                        .src()
                        .map(|s| String::from(s.path_string()))
                        .unwrap_or_else(|| String::from("None")),
                    error: err.error().to_string(),
                    debug: err.debug(),
                    source: err.error(),
                }
                .into());
            }
            message => debug!("handling message:\n{:#?}", message),
        }
    }

    pipeline.set_state(gst::State::Null)?;

    Ok(())
}

fn create_test_pipeline() -> Result<gst::Pipeline> {
    gst::init()?;

    let videotestsrc =
        gst::ElementFactory::make("videotestsrc", None).context("Cannot create videotestsrc")?;
    videotestsrc.set_property("is-live", true);

    let capsfilter =
        gst::ElementFactory::make("capsfilter", None).context("Cannot create capsfilter")?;

    let caps = gst::Caps::builder("video/x-raw")
        .field("width", 1280)
        .field("height", 720)
        .field("framerate", gst::Fraction::new(25, 1))
        .build();
    capsfilter.set_property("caps", &caps);

    let autovideosink =
        gst::ElementFactory::make("autovideosink", None).context("Cannot create autovideosink")?;

    let pipeline = gst::Pipeline::new(None);
    pipeline.add_many(&[&videotestsrc, &capsfilter, &autovideosink])?;
    gst::Element::link_many(&[&videotestsrc, &capsfilter, &autovideosink])?;

    Ok(pipeline)
}

/// Emit EOS https://gstreamer.freedesktop.org/documentation/app/appsrc.html?gi-language=c#appsrc::end-of-stream
fn create_pipeline() -> Result<gst::Pipeline> {
    gst::init()?;

    let appsrc =
        gst::ElementFactory::make("appsrc", Some("appsrc")).context("Cannot create appsrc")?;
    let rawvideoparse =
        gst::ElementFactory::make("rawvideoparse", None).context("Cannot create rawvideoparse")?;
    let x264enc = gst::ElementFactory::make("x264enc", None).context("Cannot create x264enc")?;
    let h264parse =
        gst::ElementFactory::make("h264parse", None).context("Cannot create h264parse")?;
    let hlssink2 = gst::ElementFactory::make("hlssink2", None).context("Cannot create hlssink2")?;

    appsrc.set_property("is-live", true);
    rawvideoparse.set_properties(&[
        ("use-sink-caps", &false),
        ("width", &1280_i32),
        ("height", &720_i32),
    ]);
    x264enc.set_property_from_str("tune", "zerolatency");

    let pipeline = gst::Pipeline::new(None);
    pipeline.add_many(&[&appsrc, &rawvideoparse, &x264enc, &h264parse, &hlssink2])?;
    gst::Element::link_many(&[&appsrc, &rawvideoparse, &x264enc, &h264parse, &hlssink2])?;

    Ok(pipeline)
}

fn handle_client(mut stream: TcpStream) -> Result<()> {
    info!("Handling client: {}", stream.peer_addr()?);

    if let Err(err) = handshake::handle_handshake(&mut stream) {
        error!("handshake error\r\t{}", err);
        return Ok(());
    }

    let (mut session, results) = ServerSession::new(ServerSessionConfig::new())?;

    for result in results {
        if let Some(event) = handle_result(result, &mut stream)? {
            debug!("handling event: {:#?}", event);
        }
    }

    let (sender, receiver) = mpsc::channel();

    let pipeline = create_pipeline().context("cannot create pipeline")?;
    let appsrc = pipeline
        .by_name("appsrc")
        .context("cannot get appsrc by name")?
        .dynamic_cast::<gst_app::AppSrc>()
        .expect("Source element is expected to be an appsrc!");

    debug!("spawning bus thread");
    let bus_thread = thread::spawn(move || run_bus(pipeline).expect("cannot run bus"));

    debug!("spawning buffer thread");
    let buffer_thread = thread::spawn(move || {
        for buffer in receiver.iter() {
            match appsrc.push_buffer(buffer) {
                Ok(flow) => trace!("flow success {:#?}", flow),
                Err(flow) => {
                    error!("flow error {:#?}", flow);
                    break;
                }
            }
        }
    });

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
                        if let Err(err) = sender.send(gst::Buffer::from_slice(data)) {
                            error!("send error {:#?}", err);
                            break 'client;
                        };
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

    debug!("joining buffer thread");
    buffer_thread.join().expect("buffer thread panicked");
    debug!("joining bus thread");
    bus_thread.join().expect("bus thread panicked");

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

    run_rtmp_server()?;

    Ok(())
}

fn run_rtmp_server() -> Result<()> {
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
