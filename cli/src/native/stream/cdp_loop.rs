use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use tokio::sync::{broadcast, watch, Mutex, RwLock};

use crate::native::cdp::client::CdpClient;
use crate::native::network;

use super::timestamp_ms;

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
    client_slot: Arc<RwLock<Option<Arc<CdpClient>>>>,
    client_notify: Arc<tokio::sync::Notify>,
    screencasting: Arc<Mutex<bool>>,
    client_count: Arc<Mutex<usize>>,
    cdp_session_id: Arc<RwLock<Option<String>>>,
    viewport_width: Arc<Mutex<u32>>,
    viewport_height: Arc<Mutex<u32>>,
    viewport_scale: Arc<Mutex<f64>>,
    last_frame: Arc<RwLock<Option<String>>>,
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
                let supports_screencast = eng == "chrome";

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
                                "quality": 80,
                                "maxWidth": cap_w,
                                "maxHeight": cap_h,
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

                // Correlation lives for as long as this client/session does: a reconnect legitimately loses
                // the tool name of an invocation that was already in flight.
                let mut in_flight = InFlightTools::default();

                loop {
                    tokio::select! {
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
                                            if is_main {
                                                if let Some(url) = frame.get("url").and_then(|v| v.as_str()) {
                                                    {
                                                        let mut tabs = last_tabs.write().await;
                                                        for tab in tabs.iter_mut() {
                                                            if tab.get("active").and_then(|v| v.as_bool()).unwrap_or(false) {
                                                                tab.as_object_mut().map(|o| o.insert("url".to_string(), json!(url)));
                                                            }
                                                        }
                                                    }
                                                    let msg = json!({
                                                        "type": "url",
                                                        "url": url,
                                                        "timestamp": timestamp_ms(),
                                                    });
                                                    let _ = frame_tx.send(msg.to_string());
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
                                            let msg = json!({
                                                "type": "frame",
                                                "data": data,
                                                "metadata": {
                                                    "offsetTop": meta.and_then(|m| m.get("offsetTop")).and_then(|v| v.as_f64()).unwrap_or(0.0),
                                                    "pageScaleFactor": meta.and_then(|m| m.get("pageScaleFactor")).and_then(|v| v.as_f64()).unwrap_or(1.0),
                                                    "deviceWidth": vw,
                                                    "deviceHeight": vh,
                                                    "scrollOffsetX": meta.and_then(|m| m.get("scrollOffsetX")).and_then(|v| v.as_f64()).unwrap_or(0.0),
                                                    "scrollOffsetY": meta.and_then(|m| m.get("scrollOffsetY")).and_then(|v| v.as_f64()).unwrap_or(0.0),
                                                    "timestamp": meta.and_then(|m| m.get("timestamp")).and_then(|v| v.as_u64()).unwrap_or(0),
                                                }
                                            });
                                            let msg_str = msg.to_string();
                                            {
                                                let mut lf = last_frame.write().await;
                                                *lf = Some(msg_str.clone());
                                            }
                                            let _ = frame_tx.send(msg_str);
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
