use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    protocol::{ApiProtocol, ToolDefinition},
    xml_protocol::{XmlToolCall, next_tool_fragment, parse_tool_calls},
};

// XML markers are assembled with concat! so the contiguous literal never
// appears verbatim in source; they equal the strings parse_tool_calls accepts.
const START_MARKERS: &[(&str, &str)] = &[
    (concat!('<', "tool_call"), concat!("</", "tool_call", ">")),
    (
        concat!('<', "function_calls"),
        concat!("</", "function_calls", ">"),
    ),
    (concat!('<', "invoke"), concat!("</", "invoke", ">")),
    (concat!('<', "name"), concat!("</", "tool_call", ">")),
    (concat!('<', "arguments"), concat!("</", "arguments", ">")),
];

#[derive(Debug)]
pub enum ScanEvent {
    /// Safe text that should be forwarded to the client immediately.
    Emit(String),
    /// A complete tool-call block was intercepted and parsed.
    ToolCall(XmlToolCall),
}

#[derive(Debug, Eq, PartialEq)]
pub enum OutputPiece {
    Thinking(String),
    Text(String),
}

#[derive(Default)]
enum ThinkingState {
    #[default]
    Initial,
    Thinking,
    Text,
}

#[derive(Default)]
pub struct ThinkingSplitter {
    state: ThinkingState,
    buf: String,
}

impl ThinkingSplitter {
    pub fn feed(&mut self, chunk: &str) -> Vec<OutputPiece> {
        self.buf.push_str(chunk);
        let mut out = Vec::new();

        match self.state {
            ThinkingState::Initial => {
                let lf_marker = "Thinking...\n>";
                let crlf_marker = "Thinking...\r\n>";
                if self.buf.starts_with(lf_marker) || self.buf.starts_with(crlf_marker) {
                    let marker_len = if self.buf.starts_with(lf_marker) {
                        "Thinking...\n".len()
                    } else {
                        "Thinking...\r\n".len()
                    };
                    self.buf.drain(..marker_len);
                    self.state = ThinkingState::Thinking;
                    self.flush_thinking(&mut out);
                } else if lf_marker.starts_with(&self.buf) || crlf_marker.starts_with(&self.buf) {
                    // Hold a partial leading marker until it is distinguishable.
                } else {
                    self.state = ThinkingState::Text;
                    out.push(OutputPiece::Text(std::mem::take(&mut self.buf)));
                }
            }
            ThinkingState::Thinking => self.flush_thinking(&mut out),
            ThinkingState::Text => {
                if !self.buf.is_empty() {
                    out.push(OutputPiece::Text(std::mem::take(&mut self.buf)));
                }
            }
        }

        out
    }

    pub fn finish(&mut self) -> Vec<OutputPiece> {
        let mut out = Vec::new();
        match self.state {
            ThinkingState::Initial | ThinkingState::Text => {
                if !self.buf.is_empty() {
                    out.push(OutputPiece::Text(std::mem::take(&mut self.buf)));
                }
            }
            ThinkingState::Thinking => {
                let thinking = strip_thinking_prefixes(&self.buf);
                self.buf.clear();
                if !thinking.is_empty() {
                    out.push(OutputPiece::Thinking(thinking));
                }
            }
        }
        out
    }

    fn flush_thinking(&mut self, out: &mut Vec<OutputPiece>) {
        let Some((thinking_end, separator_len)) = first_blank_line(&self.buf) else {
            return;
        };
        let thinking_raw: String = self.buf.drain(..thinking_end).collect();
        self.buf.drain(..separator_len);
        let thinking = strip_thinking_prefixes(&thinking_raw);
        if !thinking.is_empty() {
            out.push(OutputPiece::Thinking(thinking));
        }
        if !self.buf.is_empty() {
            out.push(OutputPiece::Text(std::mem::take(&mut self.buf)));
        }
        self.state = ThinkingState::Text;
    }
}

fn first_blank_line(text: &str) -> Option<(usize, usize)> {
    let lf = text.find("\n\n").map(|idx| (idx, 2));
    let crlf = text.find("\r\n\r\n").map(|idx| (idx, 4));
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn strip_thinking_prefixes(text: &str) -> String {
    text.lines()
        .map(|line| {
            line.strip_prefix('>')
                .map(|line| line.strip_prefix(' ').unwrap_or(line))
                .unwrap_or(line)
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

/// Streaming scanner that forwards upstream text to the client live but
/// intercepts XML tool-call blocks so they never reach the client. A single
/// upstream turn may contain several tool calls and interleaved prose; this
/// scanner emits both, in source order.
#[derive(Default)]
pub struct Scanner {
    buf: String,
    capturing_end: Option<&'static str>,
    tools: Vec<ToolDefinition>,
}

impl Scanner {
    pub fn new(tools: Vec<ToolDefinition>) -> Self {
        Self {
            buf: String::new(),
            capturing_end: None,
            tools,
        }
    }

    pub fn feed(&mut self, chunk: &str) -> Vec<ScanEvent> {
        self.buf.push_str(chunk);
        let mut events = Vec::new();

        loop {
            if let Some(end_marker) = self.capturing_end {
                if let Some(end) = self.buf.find(end_marker) {
                    let full: String = self.buf.drain(..end + end_marker.len()).collect();
                    self.capturing_end = None;
                    flush_parsed(&full, &self.tools, &mut events);
                    continue;
                }

                // Models often leak the *inner* structure (<name> + <arguments>) while
                // omitting the outer <tool_call> wrapper. Try the same fragment
                // locator the non-stream parser uses; if it finds a *complete*
                // (non-incomplete) orphan fragment, cut exactly there. This lets us
                // emit the ToolCall and continue processing any following text in
                // the same feed without waiting for finish().
                if let Some(frag) = next_tool_fragment(&self.buf, 0, false)
                    && frag.end > 0
                    && frag.end <= self.buf.len()
                {
                    let full: String = self.buf.drain(..frag.end).collect();
                    self.capturing_end = None;
                    flush_parsed(&full, &self.tools, &mut events);
                    continue;
                }

                return events;
            }

            if let Some((start, end_marker)) = find_start_marker(&self.buf) {
                if start > 0 {
                    let safe: String = self.buf.drain(..start).collect();
                    events.push(ScanEvent::Emit(safe));
                }
                self.capturing_end = Some(end_marker);
                continue;
            }

            let hold = ambiguous_prefix_len(&self.buf);
            let emit_len = self.buf.len() - hold;
            if emit_len > 0 {
                let safe: String = self.buf.drain(..emit_len).collect();
                events.push(ScanEvent::Emit(safe));
            }
            return events;
        }
    }

    pub fn finish(&mut self) -> Vec<ScanEvent> {
        let mut events = Vec::new();
        if !self.buf.is_empty() {
            let full = std::mem::take(&mut self.buf);
            self.capturing_end = None;
            flush_parsed(&full, &self.tools, &mut events);
        }
        events
    }
}

fn flush_parsed(full: &str, tools: &[ToolDefinition], events: &mut Vec<ScanEvent>) {
    let calls = parse_tool_calls(full, tools);
    if calls.is_empty() {
        // The block looked like a tool call but failed to parse. Forward the
        // text so nothing is silently dropped.
        events.push(ScanEvent::Emit(full.to_string()));
        return;
    }
    for call in calls {
        events.push(ScanEvent::ToolCall(call));
    }
}

fn find_start_marker(buf: &str) -> Option<(usize, &'static str)> {
    // Single scan: look for '<', then see if any START_MARKER matches at that point.
    let mut offset = 0;
    while let Some(found) = buf[offset..].find('<') {
        let start = offset + found;
        let after = start + 1;
        for &(start_marker, end_marker) in START_MARKERS {
            let marker_bytes = start_marker.as_bytes();
            // start_marker is like "<tool_call" (without the < ? wait no, from concat it includes < )
            // Actually from definition: concat!('<', "tool_call") so it is "<tool_call"
            if buf.as_bytes()[after..].starts_with(&marker_bytes[1..]) {
                let next_pos = after + marker_bytes.len() - 1;
                if let Some(next_ch) = buf.get(next_pos..).and_then(|s| s.chars().next())
                    && matches!(next_ch, '>' | '/' | ' ' | '\n' | '\r' | '\t')
                {
                    return Some((start, end_marker));
                }
            }
        }
        offset = start + 1;
    }
    None
}

/// Length of the longest non-empty suffix of `buf` that is a prefix of
/// `marker`. A full marker at chunk end is still ambiguous because the next
/// byte decides whether this is `<tool_call>` or ordinary text like
/// `<tool_callx>`.
fn ambiguous_prefix_len(buf: &str) -> usize {
    let buf_bytes = buf.as_bytes();
    START_MARKERS
        .iter()
        .map(|(marker, _)| {
            let marker_bytes = marker.as_bytes();
            let max = buf_bytes.len().min(marker_bytes.len());
            (1..=max)
                .rev()
                .find(|&len| buf_bytes[buf_bytes.len() - len..] == marker_bytes[..len])
                .unwrap_or(0)
        })
        .max()
        .unwrap_or(0)
}

fn message_start_event(model: &str) -> Value {
    let id = Uuid::new_v4();
    json!({
        "type": "message_start",
        "message": {
            "id": format!("msg_{}", id.simple()),
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": [],
            "stop_reason": Value::Null,
            "stop_sequence": Value::Null,
            "usage": {"input_tokens": 0, "output_tokens": 0},
        },
    })
}

fn response_created_event(model: &str) -> Value {
    let id = Uuid::new_v4();
    json!({
        "type": "response.created",
        "response": {
            "id": format!("resp_{}", id.simple()),
            "object": "response",
            "status": "in_progress",
            "model": model,
            "output": [],
        },
    })
}

/// Envelope events emitted before any per-protocol stream output, per protocol.
pub fn start_tool_events(protocol: ApiProtocol, model: &str) -> Vec<Value> {
    match protocol {
        ApiProtocol::Chat => Vec::new(),
        ApiProtocol::Responses => vec![response_created_event(model)],
        ApiProtocol::Messages => vec![message_start_event(model)],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CDATA_OPEN: &str = concat!("<!", "[CDATA[");
    const CDATA_CLOSE: &str = concat!("]", "]>");

    fn block(name: &str, args: &str) -> String {
        let mut s = String::new();
        s.push_str(START_MARKERS[0].0);
        s.push_str(">\n");
        s.push_str("  <name>");
        s.push_str(name);
        s.push_str("</name>\n  <arguments>");
        s.push_str(CDATA_OPEN);
        s.push_str(args);
        s.push_str(CDATA_CLOSE);
        s.push_str("</arguments>\n");
        s.push_str(START_MARKERS[0].1);
        s
    }

    fn read_tool() -> ToolDefinition {
        ToolDefinition {
            name: "Read".to_string(),
            description: "Read a file".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {"file_path": {"type": "string"}},
                "required": ["file_path"]
            }),
        }
    }

    #[test]
    fn plain_text_passes_through() {
        let mut s = Scanner::default();
        let events = s.feed("hello world");
        assert!(matches!(events[0], ScanEvent::Emit(ref t) if t == "hello world"));
    }

    #[test]
    fn holds_ambiguous_prefix_then_flushes_on_diverge() {
        let mut s = Scanner::default();
        let e1 = s.feed("answer is ");
        assert!(matches!(e1.last(), Some(ScanEvent::Emit(t)) if t == "answer is "));
        let e2 = s.feed("<to");
        assert!(e2.is_empty(), "ambiguous prefix held: {e2:?}");
        let e3 = s.feed("ol_call");
        assert!(e3.is_empty(), "still a marker prefix: {e3:?}");
        let e4 = s.feed("x>");
        assert!(
            matches!(e4[0], ScanEvent::Emit(ref t) if t.contains("<to")),
            "{e4:?}"
        );
    }

    #[test]
    fn detects_complete_tool_call_and_suppresses() {
        let mut s = Scanner::default();
        let events = s.feed(&block("echo", "{\"x\":1}"));
        assert!(
            events.iter().all(|e| matches!(e, ScanEvent::ToolCall(_))),
            "{events:?}"
        );
        let call = match &events[0] {
            ScanEvent::ToolCall(c) => c,
            _ => unreachable!(),
        };
        assert_eq!(call.name, "echo");
        assert_eq!(call.arguments["x"], 1);
    }

    #[test]
    fn detects_function_calls_block_and_suppresses_xml() {
        let mut s = Scanner::new(vec![read_tool()]);
        let events = s.feed(
            r#"<tool_call>
  <function_calls>
    <invoke name="Read">
      <parameter name="file_path">README.md</parameter>
    </invoke>
    <invoke name="Read">
      <parameter name="file_path">Cargo.toml</parameter>
    </invoke>
  </function_calls>
</tool_call>"#,
        );
        let calls: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ScanEvent::ToolCall(call) => Some(call.clone()),
                ScanEvent::Emit(text) => {
                    assert!(
                        !text.contains("<function_calls>"),
                        "function_calls XML leaked: {text:?}"
                    );
                    None
                }
            })
            .collect();

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].name, "Read");
        assert_eq!(calls[0].arguments["file_path"], "README.md");
        assert_eq!(calls[1].arguments["file_path"], "Cargo.toml");
    }

    #[test]
    fn detects_bare_function_calls_block_and_suppresses_xml() {
        let mut s = Scanner::new(vec![read_tool()]);
        let events = s.feed(
            r#"<function_calls>
  <invoke name="Read">
    <parameter name="file_path">README.md</parameter>
  </invoke>
</function_calls>"#,
        );
        let calls: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ScanEvent::ToolCall(call) => Some(call.clone()),
                ScanEvent::Emit(text) => {
                    assert!(
                        !text.contains("<function_calls>"),
                        "function_calls XML leaked: {text:?}"
                    );
                    None
                }
            })
            .collect();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Read");
        assert_eq!(calls[0].arguments["file_path"], "README.md");
    }

    #[test]
    fn detects_bare_invoke_and_suppresses_xml() {
        let mut s = Scanner::new(vec![read_tool()]);
        let events = s.feed(
            r#"<invoke name="Read">
  <parameter name="file_path">README.md</parameter>
</invoke>"#,
        );
        let calls: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ScanEvent::ToolCall(call) => Some(call.clone()),
                ScanEvent::Emit(text) => {
                    assert!(!text.contains("<invoke"), "invoke XML leaked: {text:?}");
                    None
                }
            })
            .collect();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Read");
        assert_eq!(calls[0].arguments["file_path"], "README.md");
    }

    #[test]
    fn detects_orphan_name_arguments_and_suppresses_xml() {
        let mut s = Scanner::new(vec![read_tool()]);
        let events = s.feed(
            r#"<name>Read</name>
    <arguments><![CDATA[{"file_path": "Dockerfile"}
  </tool_call>"#,
        );
        let calls: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ScanEvent::ToolCall(call) => Some(call.clone()),
                ScanEvent::Emit(text) => {
                    assert!(
                        !text.contains("<name>Read</name>"),
                        "orphan XML leaked: {text:?}"
                    );
                    None
                }
            })
            .collect();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Read");
        assert_eq!(calls[0].arguments["file_path"], "Dockerfile");
    }

    #[test]
    fn finish_parses_incomplete_tool_call() {
        let mut s = Scanner::new(vec![read_tool()]);
        assert!(
            s.feed(
                r#"<tool_call>
  <function_calls>
    <invoke name=Read>
      <parameter name=file_path>README.md"#
            )
            .is_empty()
        );
        let events = s.finish();
        let calls: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ScanEvent::ToolCall(call) => Some(call.clone()),
                ScanEvent::Emit(text) => {
                    assert!(
                        !text.contains("<tool_call>"),
                        "tool_call XML leaked: {text:?}"
                    );
                    None
                }
            })
            .collect();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Read");
        assert_eq!(calls[0].arguments["file_path"], "README.md");
    }

    #[test]
    fn detects_multiple_sequential_tool_calls_in_one_chunk() {
        let mut s = Scanner::default();
        let combined = format!("{}\n{}", block("a", "{\"x\":1}"), block("b", "{\"x\":2}"));
        let events = s.feed(&combined);
        let calls: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ScanEvent::ToolCall(c) => Some(c.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].name, "a");
        assert_eq!(calls[1].name, "b");
    }

    #[test]
    fn emits_interleaved_text_and_tool_calls() {
        let mut s = Scanner::default();
        let combined = format!(
            "prelude {} interlude {} epilogue",
            block("a", "{}"),
            block("b", "{}"),
        );
        let events = s.feed(&combined);
        let mut text = String::new();
        let mut tool_count = 0;
        for event in events {
            match event {
                ScanEvent::Emit(t) => text.push_str(&t),
                ScanEvent::ToolCall(_) => tool_count += 1,
            }
        }
        for trailing in s.finish() {
            if let ScanEvent::Emit(t) = trailing {
                text.push_str(&t);
            }
        }
        assert_eq!(tool_count, 2);
        assert!(text.contains("prelude"));
        assert!(text.contains("interlude"));
        assert!(text.contains("epilogue"));
    }

    #[test]
    fn final_answer_streams_live() {
        let mut s = Scanner::default();
        let mut out = String::new();
        for chunk in ["The ", "answer ", "is 42"] {
            for event in s.feed(chunk) {
                if let ScanEvent::Emit(t) = event {
                    out.push_str(&t);
                }
            }
        }
        assert_eq!(out, "The answer is 42");
    }

    #[test]
    fn orphan_name_arguments_without_wrapper_is_bounded_and_does_not_swallow_trailing_text() {
        let mut s = Scanner::new(vec![read_tool()]);
        // Model leaks only the inner name+arguments, no opening <tool_call> at all,
        // and there is prose both before and after.
        let input = "plan\n<name>Read</name><arguments>{\"file_path\":\"README.md\"}</arguments>\nnow do more";
        let events = s.feed(input);

        let mut emitted = String::new();
        let mut calls = Vec::new();
        for e in &events {
            match e {
                ScanEvent::Emit(t) => emitted.push_str(t),
                ScanEvent::ToolCall(c) => calls.push(c.clone()),
            }
        }
        // Finish should not be needed; the flexible detection should have cut the orphan.
        for e in s.finish() {
            match e {
                ScanEvent::Emit(t) => emitted.push_str(&t),
                ScanEvent::ToolCall(c) => calls.push(c),
            }
        }

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Read");
        assert_eq!(calls[0].arguments["file_path"], "README.md");

        // "plan\n" before + "\nnow do more" after should have been emitted as safe text.
        assert!(
            emitted.contains("plan"),
            "missing leading prose: {emitted:?}"
        );
        assert!(
            emitted.contains("now do more"),
            "trailing prose was swallowed: {emitted:?}"
        );
        // The XML itself must not leak.
        assert!(
            !emitted.contains("<name>"),
            "name tag leaked as text: {emitted:?}"
        );
    }

    #[test]
    fn multiple_orphan_calls_with_interleaved_text_are_extracted_cleanly() {
        let mut s = Scanner::new(vec![read_tool()]);
        let input = "start <name>Read</name><arguments>{\"file_path\":\"a\"}</arguments> middle <name>Read</name><arguments>{\"file_path\":\"b\"}</arguments> end";
        let events = s.feed(input);

        let mut texts = Vec::new();
        let mut calls = Vec::new();
        for e in events {
            match e {
                ScanEvent::Emit(t) => texts.push(t),
                ScanEvent::ToolCall(c) => calls.push(c),
            }
        }
        for e in s.finish() {
            match e {
                ScanEvent::Emit(t) => texts.push(t),
                ScanEvent::ToolCall(c) => calls.push(c),
            }
        }

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].arguments["file_path"], "a");
        assert_eq!(calls[1].arguments["file_path"], "b");

        let joined = texts.join("");
        assert!(joined.contains("start"));
        assert!(joined.contains("middle"));
        assert!(joined.contains("end"));
    }
}
