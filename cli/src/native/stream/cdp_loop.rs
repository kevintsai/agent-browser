use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use futures_util::FutureExt;
use tokio::sync::{broadcast, watch, Mutex, RwLock};

use crate::native::cdp::client::CdpClient;
use crate::native::network;

use super::timestamp_ms;

/// Capture time of a screencast frame, in epoch milliseconds.
///
/// CDP sends `Network.TimeSinceEpoch`, a float in seconds; reading it as an
/// integer yields 0 for every frame. Milliseconds match this protocol's other
/// timestamps.
fn frame_timestamp_ms(meta: Option<&Value>) -> u64 {
    meta.and_then(|m| m.get("timestamp"))
        .and_then(|v| v.as_f64())
        .filter(|s| *s > 0.0)
        .map(|s| (s * 1000.0) as u64)
        .unwrap_or(0)
}

fn session_matches(active_session: Option<&str>, event_session: Option<&str>) -> bool {
    match active_session {
        Some("") => event_session.is_none_or(str::is_empty),
        Some(active) => event_session == Some(active),
        None => false,
    }
}

fn main_frame_id(frame_tree: &Value) -> Option<String> {
    frame_tree
        .get("frameTree")
        .and_then(|tree| tree.get("frame"))
        .and_then(|frame| frame.get("id"))
        .and_then(Value::as_str)
        .map(String::from)
}

async fn seed_main_frame_id(
    client: Arc<CdpClient>,
    session_id: Option<String>,
    delay: std::time::Duration,
    enabled: bool,
) -> Option<String> {
    if !enabled {
        std::future::pending::<()>().await;
    }
    tokio::time::sleep(delay).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        client.send_command_no_params("Page.getFrameTree", session_id.as_deref()),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .and_then(|tree| main_frame_id(&tree))
}

async fn publish_url(
    frame_tx: &broadcast::Sender<String>,
    last_tabs: &RwLock<Vec<Value>>,
    cdp_session_id: &RwLock<Option<String>>,
    event_session_id: Option<&str>,
    url: &str,
) {
    let active_session = cdp_session_id.read().await;
    if !session_matches(active_session.as_deref(), event_session_id) {
        return;
    }
    {
        let mut tabs = last_tabs.write().await;
        for tab in tabs.iter_mut() {
            if tab.get("active").and_then(Value::as_bool).unwrap_or(false) {
                if let Some(tab) = tab.as_object_mut() {
                    tab.insert("url".to_string(), json!(url));
                }
            }
        }
    }
    let message = json!({
        "type": "url",
        "url": url,
        "timestamp": timestamp_ms(),
    });
    let _ = frame_tx.send(message.to_string());
}

/// Subscribes to the active page's CDP events and broadcasts stream updates.
///
/// Frames use `frame_watch` so the latest value wins. Other messages stay on
/// the ordered `frame_tx` channel. URL updates follow only the active main
/// frame. Chrome also includes History API and fragment navigation.
/// fleetmux fork — page tools (CDP `WebMCP` domain).
///
/// Everything a tool carries (`input`, `output`, error text) is written by the page, so it is forwarded as
/// opaque text and never parsed, reshaped or trusted here. These payloads are capped because a stream
/// message fans out to every attached viewer; a page is free to return megabytes and upstream's own limit
/// (2 MB) is far too large to broadcast. Whoever needs the whole value asks for it over a request/response
/// path instead.
pub(super) const WEBMCP_PAYLOAD_LIMIT: usize = 16 * 1024;

/// How many invocations to remember so a `toolResponded` can name its tool. `toolResponded` carries only an
/// `invocationId` — correlating here is what keeps that lookup out of every consumer downstream.
const WEBMCP_INFLIGHT_LIMIT: usize = 256;

/// Truncate a page-authored payload to [`WEBMCP_PAYLOAD_LIMIT`] bytes, cutting on a char boundary so the
/// result stays valid UTF-8. Returns the text and whether anything was dropped.
fn truncate_payload(mut text: String) -> (String, bool) {
    if text.len() <= WEBMCP_PAYLOAD_LIMIT {
        return (text, false);
    }
    let mut end = WEBMCP_PAYLOAD_LIMIT;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    (text, true)
}

/// CDP `WebMCP.InvocationStatus` (`Completed` | `Canceled` | `Error`) → the token used on the wire. The
/// domain is experimental, so an unrecognised value maps to `unknown` rather than being flattened into
/// `error` — a status we cannot read is not the same fact as a tool that failed.
fn webmcp_status(raw: &str) -> &'static str {
    match raw {
        "Completed" => "completed",
        "Canceled" => "canceled",
        "Error" => "error",
        _ => "unknown",
    }
}

/// `toolResponded` reports failure in two optional places: `errorText` for protocol users and `exception`
/// when the tool's JS threw. Prefer the former, fall back to the exception's description.
fn webmcp_error_text(params: &Value) -> Option<String> {
    if let Some(text) = params.get("errorText").and_then(|v| v.as_str()) {
        if !text.is_empty() {
            return Some(text.to_string());
        }
    }
    params
        .get("exception")
        .and_then(|e| e.get("description").and_then(|v| v.as_str()))
        .filter(|d| !d.is_empty())
        .map(|d| d.to_string())
}

/// A page-authored value as it goes on the wire: strings verbatim, anything else re-serialised as JSON text.
/// Keeping one text type means truncation has a single well-defined meaning for every payload.
fn webmcp_payload_text(value: &Value) -> String {
    match value.as_str() {
        Some(s) => s.to_string(),
        None => value.to_string(),
    }
}

/// Bounded invoked→responded correlation table (see [`WEBMCP_INFLIGHT_LIMIT`]). Entries are dropped when the
/// response arrives, or evicted oldest-first if a page starts invocations that never respond; a lost entry
/// simply leaves the tool name absent on the response.
#[derive(Default)]
struct InFlightTools {
    by_id: HashMap<String, (String, String)>,
    order: VecDeque<String>,
}

impl InFlightTools {
    fn record(&mut self, invocation: &str, tool: &str, frame: &str) {
        if self
            .by_id
            .insert(
                invocation.to_string(),
                (tool.to_string(), frame.to_string()),
            )
            .is_none()
        {
            self.order.push_back(invocation.to_string());
        }
        while self.order.len() > WEBMCP_INFLIGHT_LIMIT {
            if let Some(oldest) = self.order.pop_front() {
                self.by_id.remove(&oldest);
            }
        }
    }

    fn take(&mut self, invocation: &str) -> Option<(String, String)> {
        self.by_id.remove(invocation)
    }
}

/// Background task that subscribes to CDP events and broadcasts screencast frames in real-time.
/// Also handles auto-start/stop of screencast based on WebSocket client count.
#[allow(clippy::too_many_arguments)]
pub(super) async fn cdp_event_loop(
    frame_tx: broadcast::Sender<String>,
    frame_watch: watch::Sender<Option<Arc<super::StreamFrame>>>,
    screencast_config: Arc<super::ScreencastConfig>,
    client_slot: Arc<RwLock<Option<Arc<CdpClient>>>>,
    client_notify: Arc<tokio::sync::Notify>,
    screencasting: Arc<Mutex<bool>>,
    client_count: Arc<Mutex<usize>>,
    cdp_session_id: Arc<RwLock<Option<String>>>,
    viewport_width: Arc<Mutex<u32>>,
    viewport_height: Arc<Mutex<u32>>,
    viewport_scale: Arc<Mutex<f64>>,
    last_tabs: Arc<RwLock<Vec<Value>>>,
    last_engine: Arc<RwLock<String>>,
    recording: Arc<Mutex<bool>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    let session_id = cdp_session_id.read().await.clone();
                    if *screencasting.lock().await {
                        if let Some(ref client) = *client_slot.read().await {
                            let _ = client
                                .send_command_no_params("Page.stopScreencast", session_id.as_deref())
                                .await;
                        }
                        let mut sc = screencasting.lock().await;
                        *sc = false;
                    }
                    return;
                }
            }
            _ = client_notify.notified() => {}
        }

        let count = *client_count.lock().await;
        let guard = client_slot.read().await;

        if count > 0 {
            if let Some(ref client) = *guard {
                let mut event_rx = client.subscribe();
                let client_arc = Arc::clone(client);
                drop(guard);

                let session_id = cdp_session_id.read().await.clone();

                let vw = *viewport_width.lock().await;
                let vh = *viewport_height.lock().await;
                let vscale = *viewport_scale.lock().await;

                let eng = last_engine.read().await.clone();
                let is_chrome = eng == "chrome";
                let supports_screencast = is_chrome;
                let supports_same_document_navigation = is_chrome;

                if supports_screencast {
                    // `maxWidth`/`maxHeight` are DEVICE pixels: the compositor surface is the CSS viewport
                    // times `deviceScaleFactor`, and Chrome downscales the capture to fit the cap. Capping
                    // at the CSS size therefore throws away a Retina client's extra resolution — the client
                    // then upscales a 1x image and every glyph goes soft.
                    let (cap_w, cap_h) = super::capture_dims(vw, vh, vscale);
                    let _ = client_arc
                        .send_command(
                            "Page.startScreencast",
                            Some(json!({
                                "format": "jpeg",
                                "quality": screencast_config.quality,
                                "maxWidth": screencast_config.max_width.unwrap_or(cap_w),
                                "maxHeight": screencast_config.max_height.unwrap_or(cap_h),
                                "everyNthFrame": 1,
                            })),
                            session_id.as_deref(),
                        )
                        .await;
                }

                // fleetmux fork: page tools. `enable` replays `toolsAdded` for tools already registered —
                // ignored here, the registry is pulled on demand; only invocations are forwarded. The domain is
                // experimental and Chrome-only, so a browser without it just fails this command and the rest of
                // the loop is unaffected. No matching `disable`: invocations are rare (unlike frames), so an
                // idle subscription costs nothing worth a second teardown path in every exit branch.
                if eng == "chrome" {
                    let _ = client_arc
                        .send_command_no_params("WebMCP.enable", session_id.as_deref())
                        .await;
                }

                {
                    let mut sc = screencasting.lock().await;
                    *sc = supports_screencast;
                }

                let rec = *recording.lock().await;
                let status = json!({
                    "type": "status",
                    "connected": true,
                    "screencasting": supports_screencast,
                    "viewportWidth": vw,
                    "viewportHeight": vh,
                    "engine": eng,
                    "recording": rec,
                });
                let _ = frame_tx.send(status.to_string());

                let frame_tree_seed = seed_main_frame_id(
                    Arc::clone(&client_arc),
                    session_id.clone(),
                    std::time::Duration::ZERO,
                    supports_same_document_navigation,
                )
                .fuse();
                tokio::pin!(frame_tree_seed);
                let mut seed_in_flight = supports_same_document_navigation;
                let mut active_main_frame_id = None;
                let mut pending_same_document = VecDeque::<(Option<String>, String, String)>::new();

                // Correlation lives for as long as this client/session does: a reconnect legitimately loses
                // the tool name of an invocation that was already in flight.
                let mut in_flight = InFlightTools::default();

                loop {
                    tokio::select! {
                        seeded_frame_id = &mut frame_tree_seed => {
                            seed_in_flight = false;
                            if active_main_frame_id.is_none() {
                                active_main_frame_id = seeded_frame_id;
                            }
                            if let Some(main_frame_id) = active_main_frame_id.as_deref() {
                                for (event_session_id, frame_id, url) in
                                    pending_same_document.drain(..)
                                {
                                    if frame_id == main_frame_id {
                                        publish_url(
                                            &frame_tx,
                                            &last_tabs,
                                            &cdp_session_id,
                                            event_session_id.as_deref(),
                                            &url,
                                        )
                                        .await;
                                    }
                                }
                            } else if !pending_same_document.is_empty() {
                                frame_tree_seed.set(
                                    seed_main_frame_id(
                                        Arc::clone(&client_arc),
                                        session_id.clone(),
                                        std::time::Duration::from_millis(250),
                                        true,
                                    )
                                    .fuse(),
                                );
                                seed_in_flight = true;
                            }
                        }
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                if supports_screencast {
                                    let session_id = cdp_session_id.read().await.clone();
                                    let _ = client_arc
                                        .send_command_no_params("Page.stopScreencast", session_id.as_deref())
                                        .await;
                                }
                                let mut sc = screencasting.lock().await;
                                *sc = false;
                                return;
                            }
                        }
                        event = event_rx.recv() => {
                            match event {
                                Ok(evt) => {
                                    if evt.method == "Page.frameNavigated" {
                                        if let Some(frame) = evt.params.get("frame") {
                                            let is_main = frame
                                                .get("parentId")
                                                .and_then(|v| v.as_str())
                                                .is_none_or(|s| s.is_empty());
                                            let is_active_session = session_matches(
                                                session_id.as_deref(),
                                                evt.session_id.as_deref(),
                                            );
                                            if is_main && is_active_session {
                                                if supports_screencast {
                                                    pending_same_document.clear();
                                                    active_main_frame_id = frame
                                                        .get("id")
                                                        .and_then(Value::as_str)
                                                        .map(String::from);
                                                }
                                                if let Some(url) = frame.get("url").and_then(|v| v.as_str()) {
                                                    publish_url(
                                                        &frame_tx,
                                                        &last_tabs,
                                                        &cdp_session_id,
                                                        evt.session_id.as_deref(),
                                                        url,
                                                    )
                                                    .await;
                                                }
                                            }
                                        }
                                    } else if evt.method == "Page.navigatedWithinDocument" {
                                        let is_active_session = supports_same_document_navigation
                                            && session_matches(
                                                session_id.as_deref(),
                                                evt.session_id.as_deref(),
                                            );
                                        if is_active_session {
                                            if let (Some(frame_id), Some(url)) = (
                                                evt.params.get("frameId").and_then(Value::as_str),
                                                evt.params.get("url").and_then(Value::as_str),
                                            ) {
                                                if active_main_frame_id.is_none() {
                                                    if pending_same_document.len() == 64 {
                                                        pending_same_document.pop_front();
                                                    }
                                                    pending_same_document
                                                        .push_back((
                                                            evt.session_id.clone(),
                                                            frame_id.to_string(),
                                                            url.to_string(),
                                                        ));
                                                    if !seed_in_flight {
                                                        frame_tree_seed.set(
                                                            seed_main_frame_id(
                                                                Arc::clone(&client_arc),
                                                                session_id.clone(),
                                                                std::time::Duration::ZERO,
                                                                true,
                                                            )
                                                            .fuse(),
                                                        );
                                                        seed_in_flight = true;
                                                    }
                                                } else if Some(frame_id)
                                                    == active_main_frame_id.as_deref()
                                                {
                                                    publish_url(
                                                        &frame_tx,
                                                        &last_tabs,
                                                        &cdp_session_id,
                                                        evt.session_id.as_deref(),
                                                        url,
                                                    )
                                                    .await;
                                                }
                                            }
                                        }
                                    } else if evt.method == "Page.screencastFrame" {
                                        if let Some(sid) = evt.params.get("sessionId").and_then(|v| v.as_i64()) {
                                            let _ = client_arc.send_command(
                                                "Page.screencastFrameAck",
                                                Some(json!({ "sessionId": sid })),
                                                evt.session_id.as_deref(),
                                            ).await;
                                        }

                                        if let Some(data) = evt.params.get("data").and_then(|v| v.as_str()) {
                                            let meta = evt.params.get("metadata");
                                            let seq = super::next_frame_seq();
                                            let msg = json!({
                                                "type": "frame",
                                                "seq": seq,
                                                "data": data,
                                                "metadata": {
                                                    "offsetTop": meta.and_then(|m| m.get("offsetTop")).and_then(|v| v.as_f64()).unwrap_or(0.0),
                                                    "pageScaleFactor": meta.and_then(|m| m.get("pageScaleFactor")).and_then(|v| v.as_f64()).unwrap_or(1.0),
                                                    "deviceWidth": vw,
                                                    "deviceHeight": vh,
                                                    "scrollOffsetX": meta.and_then(|m| m.get("scrollOffsetX")).and_then(|v| v.as_f64()).unwrap_or(0.0),
                                                    "scrollOffsetY": meta.and_then(|m| m.get("scrollOffsetY")).and_then(|v| v.as_f64()).unwrap_or(0.0),
                                                    "timestamp": frame_timestamp_ms(meta),
                                                }
                                            });
                                            frame_watch.send_replace(Some(Arc::new(
                                                super::StreamFrame {
                                                    seq: Some(seq),
                                                    json: msg.to_string(),
                                                },
                                            )));
                                        }
                                    } else if evt.method == "Runtime.consoleAPICalled" {
                                        let level = evt.params.get("type")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("log");
                                        let raw_args = evt.params.get("args")
                                            .and_then(|v| v.as_array())
                                            .cloned()
                                            .unwrap_or_default();
                                        let text = network::format_console_args(&raw_args);
                                        if !text.is_empty() {
                                            let mut msg = json!({
                                                "type": "console",
                                                "level": level,
                                                "text": text,
                                                "timestamp": timestamp_ms(),
                                            });
                                            if !raw_args.is_empty() {
                                                msg.as_object_mut().unwrap().insert(
                                                    "args".to_string(),
                                                    Value::Array(raw_args),
                                                );
                                            }
                                            let _ = frame_tx.send(msg.to_string());
                                        }
                                    } else if evt.method == "Runtime.exceptionThrown" {
                                        let text = evt.params.get("exceptionDetails")
                                            .and_then(|d| {
                                                d.get("exception")
                                                    .and_then(|e| e.get("description").and_then(|v| v.as_str()))
                                                    .or_else(|| d.get("text").and_then(|v| v.as_str()))
                                            })
                                            .unwrap_or("Unknown error");
                                        let line = evt.params.get("exceptionDetails")
                                            .and_then(|d| d.get("lineNumber").and_then(|v| v.as_i64()));
                                        let column = evt.params.get("exceptionDetails")
                                            .and_then(|d| d.get("columnNumber").and_then(|v| v.as_i64()));
                                        let msg = json!({
                                            "type": "page_error",
                                            "text": text,
                                            "line": line,
                                            "column": column,
                                            "timestamp": timestamp_ms(),
                                        });
                                        let _ = frame_tx.send(msg.to_string());
                                    } else if evt.method == "WebMCP.toolInvoked" {
                                        // A page tool was invoked. The event is a broadcast: it fires whoever
                                        // called — an external agent, the page's own in-page agent, another CDP
                                        // host — so this is the one place that sees every invocation on the page.
                                        let invocation = evt.params.get("invocationId")
                                            .and_then(|v| v.as_str()).unwrap_or_default().to_string();
                                        let tool = evt.params.get("toolName")
                                            .and_then(|v| v.as_str()).unwrap_or_default().to_string();
                                        let frame = evt.params.get("frameId")
                                            .and_then(|v| v.as_str()).unwrap_or_default().to_string();
                                        // `input` is a JSON *string* in this domain (so is the JS-side
                                        // `executeTool` argument, while `WebMCP.invokeTool` wants an object).
                                        // Forwarded verbatim — the schema belongs to the page, not to us.
                                        let (input, truncated) = truncate_payload(
                                            evt.params.get("input")
                                                .and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                                        );
                                        if !invocation.is_empty() {
                                            in_flight.record(&invocation, &tool, &frame);
                                        }
                                        let msg = json!({
                                            "type": "webmcp-tool-invoked",
                                            "invocation": invocation,
                                            "tool": tool,
                                            "frame": frame,
                                            "input": input,
                                            "truncated": truncated,
                                            "timestamp": timestamp_ms(),
                                        });
                                        let _ = frame_tx.send(msg.to_string());
                                    } else if evt.method == "WebMCP.toolResponded" {
                                        // Carries only an `invocationId`; the tool name comes from the matching
                                        // invocation we saw earlier, or stays absent if we never saw it.
                                        let invocation = evt.params.get("invocationId")
                                            .and_then(|v| v.as_str()).unwrap_or_default().to_string();
                                        let status = webmcp_status(
                                            evt.params.get("status").and_then(|v| v.as_str()).unwrap_or_default(),
                                        );
                                        let (tool, frame) = match in_flight.take(&invocation) {
                                            Some((t, f)) => (Some(t), Some(f)),
                                            None => (None, None),
                                        };
                                        let output = evt.params.get("output").map(webmcp_payload_text);
                                        let error = webmcp_error_text(&evt.params);
                                        let (output, output_cut) = match output {
                                            Some(text) => {
                                                let (t, cut) = truncate_payload(text);
                                                (Some(t), cut)
                                            }
                                            None => (None, false),
                                        };
                                        let (error, error_cut) = match error {
                                            Some(text) => {
                                                let (t, cut) = truncate_payload(text);
                                                (Some(t), cut)
                                            }
                                            None => (None, false),
                                        };
                                        let msg = json!({
                                            "type": "webmcp-tool-responded",
                                            "invocation": invocation,
                                            "tool": tool,
                                            "frame": frame,
                                            "status": status,
                                            "output": output,
                                            "error": error,
                                            "truncated": output_cut || error_cut,
                                            "timestamp": timestamp_ms(),
                                        });
                                        let _ = frame_tx.send(msg.to_string());
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                                Err(broadcast::error::RecvError::Closed) => break,
                            }
                        }
                        _ = client_notify.notified() => {
                            let count = *client_count.lock().await;
                            let new_session_id = cdp_session_id.read().await.clone();
                            if count == 0 {
                                if supports_screencast {
                                    let _ = client_arc
                                        .send_command_no_params("Page.stopScreencast", session_id.as_deref())
                                        .await;
                                }
                                let mut sc = screencasting.lock().await;
                                *sc = false;
                                break;
                            }
                            let client_changed = {
                                let guard = client_slot.read().await;
                                let same = guard
                                    .as_ref()
                                    .is_some_and(|c| Arc::ptr_eq(c, &client_arc));
                                !same
                            };
                            let session_changed = new_session_id != session_id;
                            let new_vw = *viewport_width.lock().await;
                            let new_vh = *viewport_height.lock().await;
                            let new_vscale = *viewport_scale.lock().await;
                            let viewport_changed =
                                new_vw != vw || new_vh != vh || new_vscale != vscale;
                            if client_changed || session_changed || viewport_changed {
                                if supports_screencast {
                                    let _ = client_arc
                                        .send_command_no_params("Page.stopScreencast", session_id.as_deref())
                                        .await;
                                }
                                let mut sc = screencasting.lock().await;
                                *sc = false;
                                client_notify.notify_one();
                                break;
                            }
                        }
                    }
                }
            } else {
                drop(guard);
            }
        } else {
            let was_screencasting = *screencasting.lock().await;
            if was_screencasting {
                if let Some(ref client) = *guard {
                    let session_id = cdp_session_id.read().await.clone();
                    let _ = client
                        .send_command_no_params("Page.stopScreencast", session_id.as_deref())
                        .await;
                }
                let mut sc = screencasting.lock().await;
                *sc = false;
            }
            drop(guard);
        }
    }
}

pub async fn start_screencast(
    client: &CdpClient,
    session_id: &str,
    format: &str,
    quality: i32,
    max_width: i32,
    max_height: i32,
) -> Result<(), String> {
    client
        .send_command(
            "Page.startScreencast",
            Some(json!({
                "format": format,
                "quality": quality,
                "maxWidth": max_width,
                "maxHeight": max_height,
                "everyNthFrame": 1,
            })),
            Some(session_id),
        )
        .await?;
    Ok(())
}

pub async fn stop_screencast(client: &CdpClient, session_id: &str) -> Result<(), String> {
    client
        .send_command_no_params("Page.stopScreencast", Some(session_id))
        .await?;
    Ok(())
}

pub async fn ack_screencast_frame(
    client: &CdpClient,
    session_id: &str,
    screencast_session_id: i64,
) -> Result<(), String> {
    client
        .send_command(
            "Page.screencastFrameAck",
            Some(json!({ "sessionId": screencast_session_id })),
            Some(session_id),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::mpsc;
    use tokio_tungstenite::tungstenite::Message;

    async fn mock_cdp_with_seed_delay(
        main_frame_id: &str,
        seed_delay: std::time::Duration,
    ) -> (
        Arc<CdpClient>,
        mpsc::UnboundedSender<Value>,
        Arc<Mutex<Vec<String>>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "ws://127.0.0.1:{}/devtools/browser/mock",
            listener.local_addr().unwrap().port()
        );
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Value>();
        let methods = Arc::new(Mutex::new(Vec::new()));
        let recorded = methods.clone();
        let frame_id = main_frame_id.to_string();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut tx, mut rx) = ws.split();

            loop {
                tokio::select! {
                    message = rx.next() => {
                        let Some(Ok(Message::Text(text))) = message else {
                            break;
                        };
                        let command: Value = serde_json::from_str(&text).unwrap();
                        let id = command["id"].as_u64().unwrap();
                        let method = command["method"].as_str().unwrap();
                        recorded.lock().await.push(method.to_string());
                        let result = if method == "Page.getFrameTree" {
                            let delay = tokio::time::sleep(seed_delay);
                            tokio::pin!(delay);
                            loop {
                                tokio::select! {
                                    _ = &mut delay => break,
                                    event = event_rx.recv() => {
                                        let Some(event) = event else {
                                            break;
                                        };
                                        tx.send(Message::Text(event.to_string())).await.unwrap();
                                    }
                                }
                            }
                            json!({ "frameTree": { "frame": { "id": frame_id } } })
                        } else {
                            json!({})
                        };
                        tx.send(Message::Text(json!({ "id": id, "result": result }).to_string()))
                            .await
                            .unwrap();
                    }
                    event = event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        tx.send(Message::Text(event.to_string())).await.unwrap();
                    }
                }
            }
        });

        let client = Arc::new(CdpClient::connect(&url).await.unwrap());
        (client, event_tx, methods)
    }

    async fn mock_cdp(
        main_frame_id: &str,
    ) -> (
        Arc<CdpClient>,
        mpsc::UnboundedSender<Value>,
        Arc<Mutex<Vec<String>>>,
    ) {
        mock_cdp_with_seed_delay(main_frame_id, std::time::Duration::ZERO).await
    }

    async fn mock_cdp_with_first_seed_timeout(
        main_frame_id: &str,
    ) -> (
        Arc<CdpClient>,
        mpsc::UnboundedSender<Value>,
        Arc<Mutex<Vec<String>>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "ws://127.0.0.1:{}/devtools/browser/retry",
            listener.local_addr().unwrap().port()
        );
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Value>();
        let methods = Arc::new(Mutex::new(Vec::new()));
        let recorded = methods.clone();
        let frame_id = main_frame_id.to_string();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut tx, mut rx) = ws.split();
            let mut frame_tree_requests = 0;

            loop {
                tokio::select! {
                    message = rx.next() => {
                        let Some(Ok(Message::Text(text))) = message else {
                            break;
                        };
                        let command: Value = serde_json::from_str(&text).unwrap();
                        let id = command["id"].as_u64().unwrap();
                        let method = command["method"].as_str().unwrap();
                        recorded.lock().await.push(method.to_string());
                        if method == "Page.getFrameTree" {
                            frame_tree_requests += 1;
                            if frame_tree_requests == 1 {
                                continue;
                            }
                            tx.send(Message::Text(json!({
                                "id": id,
                                "result": { "frameTree": { "frame": { "id": frame_id } } }
                            }).to_string()))
                            .await
                            .unwrap();
                        } else {
                            tx.send(Message::Text(json!({ "id": id, "result": {} }).to_string()))
                                .await
                                .unwrap();
                        }
                    }
                    event = event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        tx.send(Message::Text(event.to_string())).await.unwrap();
                    }
                }
            }
        });

        let client = Arc::new(CdpClient::connect(&url).await.unwrap());
        (client, event_tx, methods)
    }

    async fn silent_frame_tree_cdp() -> Arc<CdpClient> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "ws://127.0.0.1:{}/devtools/browser/silent",
            listener.local_addr().unwrap().port()
        );
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut tx, mut rx) = ws.split();
            while let Some(Ok(message)) = rx.next().await {
                match message {
                    Message::Text(text) => {
                        let command: Value = serde_json::from_str(&text).unwrap();
                        let method = command["method"].as_str().unwrap();
                        if method != "Page.getFrameTree" {
                            let id = command["id"].as_u64().unwrap();
                            tx.send(Message::Text(json!({ "id": id, "result": {} }).to_string()))
                                .await
                                .unwrap();
                        }
                    }
                    Message::Ping(payload) => {
                        tx.send(Message::Pong(payload)).await.unwrap();
                    }
                    _ => {}
                }
            }
        });
        Arc::new(CdpClient::connect(&url).await.unwrap())
    }

    struct LoopHarness {
        events: mpsc::UnboundedSender<Value>,
        messages: broadcast::Receiver<String>,
        last_tabs: Arc<RwLock<Vec<Value>>>,
        cdp_session_id: Arc<RwLock<Option<String>>>,
        shutdown: watch::Sender<bool>,
        task: tokio::task::JoinHandle<()>,
        methods: Arc<Mutex<Vec<String>>>,
    }

    async fn start_loop_with_seed_delay(
        active_session: Option<&str>,
        main_frame_id: &str,
        seed_delay: std::time::Duration,
    ) -> LoopHarness {
        let (client, events, methods) = mock_cdp_with_seed_delay(main_frame_id, seed_delay).await;
        start_loop_with_client(active_session, client, events, methods).await
    }

    async fn start_loop_with_client(
        active_session: Option<&str>,
        client: Arc<CdpClient>,
        events: mpsc::UnboundedSender<Value>,
        methods: Arc<Mutex<Vec<String>>>,
    ) -> LoopHarness {
        start_loop_with_client_and_engine(active_session, client, events, methods, "chrome").await
    }

    async fn start_loop_with_client_and_engine(
        active_session: Option<&str>,
        client: Arc<CdpClient>,
        events: mpsc::UnboundedSender<Value>,
        methods: Arc<Mutex<Vec<String>>>,
        engine: &str,
    ) -> LoopHarness {
        let (frame_tx, messages) = broadcast::channel(64);
        let (frame_watch, _) = watch::channel(None);
        let client_slot = Arc::new(RwLock::new(Some(client)));
        let client_notify = Arc::new(tokio::sync::Notify::new());
        let client_count = Arc::new(Mutex::new(1));
        let cdp_session_id = Arc::new(RwLock::new(active_session.map(String::from)));
        let last_tabs = Arc::new(RwLock::new(vec![
            json!({ "tabId": "t1", "url": "https://active.test/", "active": true }),
            json!({ "tabId": "t2", "url": "https://background.test/", "active": false }),
        ]));
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(cdp_event_loop(
            frame_tx,
            frame_watch,
            Arc::new(super::super::ScreencastConfig::default()),
            client_slot,
            client_notify.clone(),
            Arc::new(Mutex::new(false)),
            client_count,
            cdp_session_id.clone(),
            Arc::new(Mutex::new(1280)),
            Arc::new(Mutex::new(720)),
            // fork: capture scale (see `capture_dims`); 1.0 keeps upstream's CSS-pixel behaviour.
            Arc::new(Mutex::new(1.0f64)),
            last_tabs.clone(),
            Arc::new(RwLock::new(engine.to_string())),
            Arc::new(Mutex::new(false)),
            shutdown_rx,
        ));
        client_notify.notify_one();

        LoopHarness {
            events,
            messages,
            last_tabs,
            cdp_session_id,
            shutdown,
            task,
            methods,
        }
    }

    async fn start_loop(active_session: Option<&str>, main_frame_id: &str) -> LoopHarness {
        start_loop_with_seed_delay(active_session, main_frame_id, std::time::Duration::ZERO).await
    }

    async fn next_message_of_type(messages: &mut broadcast::Receiver<String>, kind: &str) -> Value {
        loop {
            let text = tokio::time::timeout(std::time::Duration::from_secs(5), messages.recv())
                .await
                .expect("timed out waiting for stream message")
                .expect("stream channel closed");
            let message: Value = serde_json::from_str(&text).unwrap();
            if message["type"] == kind {
                return message;
            }
        }
    }

    async fn expect_no_url(messages: &mut broadcast::Receiver<String>) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(250);
        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let result = tokio::time::timeout(remaining, messages.recv()).await;
            let Ok(Ok(text)) = result else {
                return;
            };
            let message: Value = serde_json::from_str(&text).unwrap();
            assert_ne!(message["type"], "url", "unexpected URL message: {message}");
        }
    }

    async fn stop_loop(harness: LoopHarness) {
        let _ = harness.shutdown.send(true);
        harness.task.await.unwrap();
    }

    async fn wait_for_method(methods: &Mutex<Vec<String>>, expected: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if methods.lock().await.iter().any(|method| method == expected) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for CDP method");
    }

    /// Reading the float as an integer stamps every frame 0, so no client can
    /// measure frame age.
    #[test]
    fn test_frame_timestamp_converts_cdp_seconds_to_millis() {
        let meta = json!({ "timestamp": 1785038682.238_f64 });
        assert_eq!(frame_timestamp_ms(Some(&meta)), 1785038682238);
    }

    #[test]
    fn test_frame_timestamp_absent_or_zero_stays_zero() {
        assert_eq!(frame_timestamp_ms(None), 0);
        assert_eq!(frame_timestamp_ms(Some(&json!({}))), 0);
        assert_eq!(frame_timestamp_ms(Some(&json!({ "timestamp": 0 }))), 0);
        assert_eq!(frame_timestamp_ms(Some(&json!({ "timestamp": "nope" }))), 0);
    }

    #[tokio::test]
    async fn test_same_document_navigation_tracks_active_main_frame_and_ignores_child() {
        let mut harness = start_loop(Some("S-ACTIVE"), "F-MAIN").await;
        next_message_of_type(&mut harness.messages, "status").await;

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frameId": "F-MAIN",
                    "url": "https://active.test/spa",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();
        let history = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(history["url"], "https://active.test/spa");

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frameId": "F-MAIN",
                    "url": "https://active.test/spa#section",
                    "navigationType": "fragment"
                }
            }))
            .unwrap();
        let fragment = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(fragment["url"], "https://active.test/spa#section");

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frameId": "F-CHILD",
                    "url": "https://active.test/child",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();
        expect_no_url(&mut harness.messages).await;

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-BACKGROUND",
                "params": {
                    "frameId": "F-BACKGROUND",
                    "url": "https://background.test/spa",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();
        expect_no_url(&mut harness.messages).await;
        assert_eq!(
            harness.last_tabs.read().await[0]["url"],
            "https://active.test/spa#section"
        );
        assert!(harness
            .methods
            .lock()
            .await
            .iter()
            .any(|method| method == "Page.getFrameTree"));

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_lightpanda_background_full_navigation_does_not_replace_active_url() {
        let (client, events, methods) = mock_cdp("F-MAIN").await;
        let mut harness = start_loop_with_client_and_engine(
            Some("S-ACTIVE"),
            client,
            events,
            methods,
            "lightpanda",
        )
        .await;
        next_message_of_type(&mut harness.messages, "status").await;

        harness
            .events
            .send(json!({
                "method": "Page.frameNavigated",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frame": {
                        "id": "F-MAIN",
                        "url": "https://active.test/full"
                    }
                }
            }))
            .unwrap();
        let active = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(active["url"], "https://active.test/full");

        harness
            .events
            .send(json!({
                "method": "Page.frameNavigated",
                "sessionId": "S-BACKGROUND",
                "params": {
                    "frame": {
                        "id": "F-BACKGROUND",
                        "url": "https://background.test/changed"
                    }
                }
            }))
            .unwrap();
        expect_no_url(&mut harness.messages).await;
        assert_eq!(
            harness.last_tabs.read().await[0]["url"],
            "https://active.test/full"
        );

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_old_session_event_cannot_replace_new_active_tab_url() {
        let mut harness = start_loop(Some("S-OLD"), "F-OLD").await;
        next_message_of_type(&mut harness.messages, "status").await;

        {
            let mut session = harness.cdp_session_id.write().await;
            let mut tabs = harness.last_tabs.write().await;
            *session = Some("S-NEW".to_string());
            *tabs = vec![
                json!({ "tabId": "t1", "url": "https://old.test/", "active": false }),
                json!({ "tabId": "t2", "url": "https://new.test/", "active": true }),
            ];
        }

        harness
            .events
            .send(json!({
                "method": "Page.frameNavigated",
                "sessionId": "S-OLD",
                "params": {
                    "frame": {
                        "id": "F-OLD",
                        "url": "https://old.test/late"
                    }
                }
            }))
            .unwrap();
        expect_no_url(&mut harness.messages).await;
        assert_eq!(
            harness.last_tabs.read().await[1]["url"],
            "https://new.test/"
        );

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_same_document_navigation_retries_after_seed_timeout() {
        let (client, events, methods) = mock_cdp_with_first_seed_timeout("F-MAIN").await;
        let mut harness = start_loop_with_client(Some("S-ACTIVE"), client, events, methods).await;
        next_message_of_type(&mut harness.messages, "status").await;
        wait_for_method(&harness.methods, "Page.getFrameTree").await;

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frameId": "F-MAIN",
                    "url": "https://active.test/recovered",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();

        let message = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(message["url"], "https://active.test/recovered");
        assert_eq!(
            harness
                .methods
                .lock()
                .await
                .iter()
                .filter(|method| method.as_str() == "Page.getFrameTree")
                .count(),
            2
        );

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_same_document_navigation_waits_for_main_frame_seed() {
        let mut harness = start_loop_with_seed_delay(
            Some("S-ACTIVE"),
            "F-MAIN",
            std::time::Duration::from_millis(100),
        )
        .await;
        next_message_of_type(&mut harness.messages, "status").await;
        wait_for_method(&harness.methods, "Page.getFrameTree").await;

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frameId": "F-MAIN",
                    "url": "https://active.test/immediate",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();
        let message = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(message["url"], "https://active.test/immediate");

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_same_document_seed_buffer_discards_oldest_event_at_capacity() {
        let mut harness = start_loop_with_seed_delay(
            Some("S-ACTIVE"),
            "F-MAIN",
            std::time::Duration::from_millis(100),
        )
        .await;
        next_message_of_type(&mut harness.messages, "status").await;
        wait_for_method(&harness.methods, "Page.getFrameTree").await;

        for index in 0..65 {
            harness
                .events
                .send(json!({
                    "method": "Page.navigatedWithinDocument",
                    "sessionId": "S-ACTIVE",
                    "params": {
                        "frameId": "F-MAIN",
                        "url": format!("https://active.test/{index}"),
                        "navigationType": "historyApi"
                    }
                }))
                .unwrap();
        }

        let first = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(first["url"], "https://active.test/1");

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_background_full_navigation_does_not_replace_active_url() {
        let mut harness = start_loop(Some("S-ACTIVE"), "F-MAIN").await;
        next_message_of_type(&mut harness.messages, "status").await;

        harness
            .events
            .send(json!({
                "method": "Page.frameNavigated",
                "sessionId": "S-ACTIVE",
                "params": {
                    "frame": {
                        "id": "F-MAIN",
                        "url": "https://active.test/full"
                    }
                }
            }))
            .unwrap();
        let active = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(active["url"], "https://active.test/full");

        harness
            .events
            .send(json!({
                "method": "Page.frameNavigated",
                "sessionId": "S-BACKGROUND",
                "params": {
                    "frame": {
                        "id": "F-BACKGROUND",
                        "url": "https://background.test/changed"
                    }
                }
            }))
            .unwrap();
        expect_no_url(&mut harness.messages).await;
        assert_eq!(
            harness.last_tabs.read().await[0]["url"],
            "https://active.test/full"
        );

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_direct_page_navigation_matches_empty_active_session() {
        let mut harness = start_loop(Some(""), "F-DIRECT").await;
        next_message_of_type(&mut harness.messages, "status").await;

        harness
            .events
            .send(json!({
                "method": "Page.navigatedWithinDocument",
                "params": {
                    "frameId": "F-DIRECT",
                    "url": "https://provider.test/spa",
                    "navigationType": "historyApi"
                }
            }))
            .unwrap();
        let message = next_message_of_type(&mut harness.messages, "url").await;
        assert_eq!(message["url"], "https://provider.test/spa");

        stop_loop(harness).await;
    }

    #[tokio::test]
    async fn test_unanswered_frame_tree_seed_does_not_block_shutdown() {
        let client = silent_frame_tree_cdp().await;
        let (frame_tx, mut messages) = broadcast::channel(64);
        let (frame_watch, _) = watch::channel(None);
        let client_notify = Arc::new(tokio::sync::Notify::new());
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(cdp_event_loop(
            frame_tx,
            frame_watch,
            Arc::new(super::super::ScreencastConfig::default()),
            Arc::new(RwLock::new(Some(client.clone()))),
            client_notify.clone(),
            Arc::new(Mutex::new(false)),
            Arc::new(Mutex::new(1)),
            Arc::new(RwLock::new(Some("S-ACTIVE".to_string()))),
            Arc::new(Mutex::new(1280)),
            Arc::new(Mutex::new(720)),
            // fork: capture scale (see `capture_dims`); 1.0 keeps upstream's CSS-pixel behaviour.
            Arc::new(Mutex::new(1.0f64)),
            Arc::new(RwLock::new(Vec::new())),
            Arc::new(RwLock::new("chrome".to_string())),
            Arc::new(Mutex::new(false)),
            shutdown_rx,
        ));
        client_notify.notify_one();

        next_message_of_type(&mut messages, "status").await;
        let started = tokio::time::Instant::now();
        let _ = shutdown.send(true);
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .expect("frame tree seed should not hold shutdown for the CDP timeout")
            .unwrap();
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "shutdown waited for the default CDP command timeout"
        );
        assert_eq!(client.pending_len().await, 0);
    }
}

#[cfg(test)]
mod webmcp_tests {
    use super::*;

    #[test]
    fn short_payloads_pass_through_untouched() {
        let (text, cut) = truncate_payload("{\"t\":\"hi\"}".to_string());
        assert_eq!(text, "{\"t\":\"hi\"}");
        assert!(!cut);
    }

    #[test]
    fn oversized_payloads_are_cut_and_flagged() {
        let (text, cut) = truncate_payload("x".repeat(WEBMCP_PAYLOAD_LIMIT + 10));
        assert_eq!(text.len(), WEBMCP_PAYLOAD_LIMIT);
        assert!(cut);
    }

    #[test]
    fn truncation_never_splits_a_character() {
        // A 3-byte char straddling the limit must be dropped whole: a split would put invalid UTF-8 on the
        // wire and the viewer would fail to decode the whole message, not just the payload.
        let mut text = "a".repeat(WEBMCP_PAYLOAD_LIMIT - 1);
        text.push('界');
        let (cut_text, cut) = truncate_payload(text);
        assert!(cut);
        assert_eq!(cut_text.len(), WEBMCP_PAYLOAD_LIMIT - 1);
        assert!(cut_text.chars().all(|c| c == 'a'));
    }

    #[test]
    fn status_maps_the_domain_enum_and_keeps_unknowns_distinct() {
        assert_eq!(webmcp_status("Completed"), "completed");
        assert_eq!(webmcp_status("Canceled"), "canceled");
        assert_eq!(webmcp_status("Error"), "error");
        // Experimental domain: a value we do not know must not be reported as a failed tool.
        assert_eq!(webmcp_status("SomethingNew"), "unknown");
    }

    #[test]
    fn error_text_prefers_error_text_then_the_exception() {
        let both = json!({
            "errorText": "tool blew up",
            "exception": { "description": "TypeError: x" },
        });
        assert_eq!(webmcp_error_text(&both).as_deref(), Some("tool blew up"));

        let only_exception = json!({ "exception": { "description": "TypeError: x" } });
        assert_eq!(
            webmcp_error_text(&only_exception).as_deref(),
            Some("TypeError: x")
        );

        assert_eq!(webmcp_error_text(&json!({ "status": "Completed" })), None);
        // An empty errorText is not an error text.
        assert_eq!(webmcp_error_text(&json!({ "errorText": "" })), None);
    }

    #[test]
    fn payload_text_keeps_strings_verbatim_and_serialises_the_rest() {
        assert_eq!(webmcp_payload_text(&json!("already text")), "already text");
        assert_eq!(
            webmcp_payload_text(&json!({ "content": [{ "type": "text", "text": "hi" }] })),
            "{\"content\":[{\"text\":\"hi\",\"type\":\"text\"}]}"
        );
    }

    #[test]
    fn correlation_returns_the_tool_once_then_forgets_it() {
        let mut table = InFlightTools::default();
        table.record("inv-1", "add-to-cart", "frame-a");
        assert_eq!(
            table.take("inv-1"),
            Some(("add-to-cart".to_string(), "frame-a".to_string()))
        );
        // A second response for the same invocation has nothing to correlate against.
        assert_eq!(table.take("inv-1"), None);
    }

    #[test]
    fn correlation_evicts_oldest_and_never_grows_past_the_cap() {
        let mut table = InFlightTools::default();
        for i in 0..(WEBMCP_INFLIGHT_LIMIT + 5) {
            table.record(&format!("inv-{i}"), "t", "f");
        }
        assert_eq!(table.by_id.len(), WEBMCP_INFLIGHT_LIMIT);
        // The first five invocations were evicted; a late response for them reports no tool name.
        assert_eq!(table.take("inv-0"), None);
        assert!(table
            .take(&format!("inv-{}", WEBMCP_INFLIGHT_LIMIT + 4))
            .is_some());
    }

    #[test]
    fn re_recording_an_invocation_does_not_double_count_the_queue() {
        let mut table = InFlightTools::default();
        table.record("inv-1", "t", "f");
        table.record("inv-1", "t2", "f2");
        assert_eq!(table.order.len(), 1);
        assert_eq!(
            table.take("inv-1"),
            Some(("t2".to_string(), "f2".to_string()))
        );
    }
}
