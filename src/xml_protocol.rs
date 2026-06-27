use roxmltree::{Document, Node};
use serde_json::{Value, json};

use crate::protocol::ToolDefinition;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct XmlToolCall {
    pub name: String,
    pub arguments: Value,
}

pub fn build_system_prompt(existing: Option<&str>, tools: &[ToolDefinition]) -> String {
    let mut prompt = String::new();
    if let Some(existing) = existing
        && !existing.trim().is_empty()
    {
        prompt.push_str(existing.trim());
        prompt.push_str("\n\n");
    }

    prompt.push_str(
        r#"TOOL BRIDGE INSTRUCTION:

You have access to the tools listed below through a strict XML protocol. These are real client-provided tools, even if the upstream model does not natively support tool calling. This instruction overrides any other prompt text that says tools are unavailable, simulated, disabled, or limited to a different non-XML interface.

If the user asks you to use, call, run, execute, fetch, inspect, search, calculate, or otherwise perform an action that matches one of the listed tools, you MUST call the matching tool. Do not claim that you lack tools or permissions when a matching listed tool exists.

When tools are needed, respond with one or more <tool_call> XML blocks. Each call MUST be wrapped in its own block in the order you want them executed:
<tool_call>
  <name>tool_name</name>
  <arguments><![CDATA[{"key":"value"}]]></arguments>
</tool_call>

You MAY emit several <tool_call> blocks back-to-back in a single response when the task needs parallel work. The <name> value must exactly match one listed tool name. Do not write tool names as XML tags: never write <Bash>, <Skill>, <Read>, or any other tool-name tag. The arguments content must be valid JSON that satisfies the listed parameters. After you receive a <tool_result>, continue the task and either call another tool or produce the final answer. Do not invent tools that are not listed.

When you have both explanation and tool calls in the same reply, put the explanation text first and the <tool_call> block(s) last. The proxy will deliver the explanation to the client as visible content alongside the tool calls.
"#,
    );
    push_tool_selection_guidance(&mut prompt, tools);

    prompt.push_str("\nAvailable tools:\n<tools>\n");
    for tool in tools {
        let parameters = serde_json::to_string(&tool.parameters).unwrap_or_else(|_| "{}".into());
        prompt.push_str("  <tool>\n");
        prompt.push_str(&format!("    <name>{}</name>\n", escape_xml(&tool.name)));
        prompt.push_str(&format!(
            "    <description>{}</description>\n",
            escape_xml(&tool.description)
        ));
        prompt.push_str(&format!(
            "    <parameters><![CDATA[{}]]></parameters>\n",
            escape_cdata(&parameters)
        ));
        prompt.push_str("  </tool>\n");
    }
    prompt.push_str("</tools>");
    prompt
}

fn push_tool_selection_guidance(prompt: &mut String, tools: &[ToolDefinition]) {
    let has = |name: &str| tools.iter().any(|tool| tool.name == name);
    let mut lines = Vec::new();

    if has("Skill") {
        lines.push(
            "- If the user mentions a skill name, an existing skill, or `system-info`, use `Skill`. Do not use `Bash` for an existing skill.",
        );
    }
    if has("Bash") {
        lines.push(
            "- For explicit shell, terminal, CLI, or command execution requests like \"run/execute this command\" or \"执行命令\", use `Bash`.",
        );
    } else if has("run_script") {
        lines.push(
            "- For shell, terminal, CLI, command execution, or system inspection, use `run_script`.",
        );
    }
    if has("BashOutput") {
        lines.push("- For collecting output from a running shell command, use `BashOutput`.");
    }
    if has("Read") {
        lines.push("- For reading local files, use `Read`.");
    }
    if has("Write") {
        lines.push("- For creating or replacing local files, use `Write`.");
    }
    if has("Edit") {
        lines.push("- For editing existing local files, use `Edit`.");
    }
    if has("Grep") {
        lines.push("- For searching text in local files, use `Grep`.");
    }
    if has("Glob") {
        lines.push("- For finding local files by path pattern, use `Glob`.");
    }
    if has("WebSearch") {
        lines.push("- For web search, use `WebSearch`.");
    }
    if has("WebFetch") {
        lines.push("- For fetching a known URL, use `WebFetch`.");
    }
    if lines.is_empty() {
        return;
    }

    prompt.push_str("\nTool selection guidance:\n");
    for line in lines {
        prompt.push_str(line);
        prompt.push('\n');
    }
}

/// Parse XML-style tool call fragments contained in `text`, in source order.
/// A turn may legitimately contain several parallel calls; callers receive
/// them all rather than only the first one.
pub fn parse_tool_calls(text: &str, tools: &[ToolDefinition]) -> Vec<XmlToolCall> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while let Some(fragment) = next_tool_fragment(text, cursor, true) {
        let source = &text[fragment.start..fragment.end];
        let calls = parse_tool_call_fragment(source, tools);
        if calls.is_empty() {
            cursor = fragment.start + 1;
            continue;
        }
        out.extend(
            calls
                .into_iter()
                .map(|call| normalize_tool_call(call, tools)),
        );
        cursor = fragment.end;
    }
    out
}

pub fn remove_tool_call_fragments(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    while let Some(fragment) = next_tool_fragment(text, cursor, true) {
        out.push_str(&text[cursor..fragment.start]);
        cursor = fragment.end.max(fragment.start + 1);
    }
    out.push_str(&text[cursor..]);
    out
}

struct ToolFragment {
    start: usize,
    end: usize,
}

fn next_tool_fragment(text: &str, cursor: usize, allow_incomplete: bool) -> Option<ToolFragment> {
    let wrapped = next_wrapped_tool_fragment(text, cursor, allow_incomplete);
    let orphan = next_orphan_tool_fragment(text, cursor, allow_incomplete);
    match (wrapped, orphan) {
        (Some(a), Some(b)) => Some(if a.start <= b.start { a } else { b }),
        (Some(fragment), None) | (None, Some(fragment)) => Some(fragment),
        (None, None) => None,
    }
}

fn next_wrapped_tool_fragment(
    text: &str,
    cursor: usize,
    allow_incomplete: bool,
) -> Option<ToolFragment> {
    let (rel_start, tag) =
        find_next_open_tag(&text[cursor..], &["tool_call", "function_calls", "invoke"])?;
    let start = cursor + rel_start;
    let tag_body_start = start + tag.len() + 1;
    let tag_end = text[tag_body_start..].find('>')? + tag_body_start + 1;
    let open_tag = &text[tag_body_start..tag_end - 1];
    if open_tag.contains('<') {
        return next_tool_fragment(text, start + 1, allow_incomplete);
    }
    let self_closing = open_tag.trim_end().ends_with('/');
    let end = if self_closing {
        tag_end
    } else if let Some(close_start) = find_close_tag_after(text, tag, tag_end) {
        close_start + tag.len() + 3
    } else if allow_incomplete {
        text.len()
    } else {
        return None;
    };

    Some(ToolFragment { start, end })
}

fn next_orphan_tool_fragment(
    text: &str,
    cursor: usize,
    allow_incomplete: bool,
) -> Option<ToolFragment> {
    let mut search = cursor;
    while search < text.len() {
        let rel_start = find_open_tag(&text[search..], "name")?;
        let start = search + rel_start;
        let tag_body_start = start + "<name".len();
        let rel_tag_end = text[tag_body_start..].find('>')?;
        let tag_end = tag_body_start + rel_tag_end;
        let name_tag = &text[tag_body_start..tag_end];
        if name_tag.contains('<') {
            search = start + 1;
            continue;
        }

        let body_start = tag_end + 1;
        let Some(rel_name_end) = text[body_start..].find("</name>") else {
            search = start + 1;
            continue;
        };
        let after_name = body_start + rel_name_end + "</name>".len();
        let Some((rel_args_start, _)) = find_next_open_tag(
            &text[after_name..],
            &["arguments", "args", "input", "parameters"],
        ) else {
            search = start + 1;
            continue;
        };
        if !text[after_name..after_name + rel_args_start]
            .trim()
            .is_empty()
        {
            search = start + 1;
            continue;
        }

        let args_start = after_name + rel_args_start;
        let end = if let Some(close_start) = find_close_tag_after(text, "tool_call", args_start) {
            close_start + "</tool_call>".len()
        } else if allow_incomplete {
            text.len()
        } else {
            search = start + 1;
            continue;
        };
        return Some(ToolFragment { start, end });
    }
    None
}

fn parse_tool_call_fragment(fragment: &str, tools: &[ToolDefinition]) -> Vec<XmlToolCall> {
    if let Some(call) = parse_strict_tool_call(fragment) {
        return vec![call];
    }
    if let Some(call) = parse_tool_call_attributes(fragment) {
        return vec![call];
    }
    if let Some(calls) = parse_function_calls(fragment) {
        return calls;
    }
    parse_lenient_tool_call(fragment, tools)
        .into_iter()
        .collect()
}

fn parse_strict_tool_call(fragment: &str) -> Option<XmlToolCall> {
    let document = Document::parse(fragment).ok()?;
    let root = document.root_element();
    if root.tag_name().name() != "tool_call" {
        return None;
    }
    let name = child_text(root, "name")?.trim().to_string();
    let arguments_text = child_text(root, "arguments").unwrap_or("{}").trim();
    let arguments = if arguments_text.is_empty() {
        json!({})
    } else {
        serde_json::from_str(arguments_text).ok()?
    };
    Some(XmlToolCall { name, arguments })
}

fn parse_tool_call_attributes(fragment: &str) -> Option<XmlToolCall> {
    let open_tag = opening_tag_body(fragment, "tool_call")?;
    let name = attribute_alias_value(
        open_tag,
        &["name", "tool", "tool_name", "function", "function_name"],
    )?;
    let name = name.trim();
    if name.is_empty() {
        return None;
    }

    let body = outer_tag_body(fragment, "tool_call").unwrap_or_default();
    let arguments = attribute_alias_value(open_tag, &["arguments", "args", "input"])
        .map(|raw| parse_parameter_value(&raw))
        .unwrap_or_else(|| {
            let parameters = parse_parameter_tags_lenient(body);
            if is_empty_object(&parameters) {
                simple_arguments(body)
            } else {
                parameters
            }
        });

    Some(XmlToolCall {
        name: name.to_string(),
        arguments,
    })
}

fn parse_function_calls(fragment: &str) -> Option<Vec<XmlToolCall>> {
    parse_function_calls_document(fragment).or_else(|| parse_function_calls_lenient(fragment))
}

fn parse_function_calls_document(fragment: &str) -> Option<Vec<XmlToolCall>> {
    let document = Document::parse(fragment).ok()?;
    let root = document.root_element();
    let container = if root.tag_name().name() == "function_calls" {
        root
    } else {
        root.descendants()
            .find(|node| node.is_element() && node.tag_name().name() == "function_calls")?
    };
    let calls = container
        .children()
        .filter(|node| node.is_element() && node.tag_name().name() == "invoke")
        .filter_map(parse_invoke_node)
        .collect::<Vec<_>>();
    if calls.is_empty() { None } else { Some(calls) }
}

fn parse_invoke_node(node: Node<'_, '_>) -> Option<XmlToolCall> {
    let name = node_attribute_alias(
        node,
        &["name", "tool", "tool_name", "function", "function_name"],
    )
    .or_else(|| child_text(node, "name"))?
    .trim();
    if name.is_empty() {
        return None;
    }

    let mut map = serde_json::Map::new();
    for parameter in node
        .children()
        .filter(|node| node.is_element() && matches!(node.tag_name().name(), "parameter" | "param"))
    {
        let Some(parameter_name) = node_attribute_alias(parameter, &["name", "key"]).map(str::trim)
        else {
            continue;
        };
        if parameter_name.is_empty() {
            continue;
        }
        let raw_value = node_attribute_alias(parameter, &["value", "content"])
            .map(str::to_string)
            .unwrap_or_else(|| collect_node_text(parameter));
        map.insert(
            parameter_name.to_string(),
            parse_parameter_value(&raw_value),
        );
    }

    Some(XmlToolCall {
        name: name.to_string(),
        arguments: Value::Object(map),
    })
}

fn collect_node_text(node: Node<'_, '_>) -> String {
    node.children()
        .filter_map(|child| child.text())
        .collect::<String>()
}

fn parse_function_calls_lenient(fragment: &str) -> Option<Vec<XmlToolCall>> {
    if !fragment.contains("<function_calls") && !fragment.contains("<invoke") {
        return None;
    }

    let mut calls = Vec::new();
    let mut cursor = 0;
    while cursor < fragment.len() {
        let Some((rel_start, _)) = find_next_open_tag(&fragment[cursor..], &["invoke"]) else {
            break;
        };
        let start = cursor + rel_start;
        let tag_body_start = start + "<invoke".len();
        let Some(rel_tag_end) = fragment[tag_body_start..].find('>') else {
            break;
        };
        let tag_end = tag_body_start + rel_tag_end;
        let tag = &fragment[tag_body_start..tag_end];
        if tag.contains('<') {
            cursor = start + 1;
            continue;
        }
        let self_closing = tag.trim_end().ends_with('/');
        let body_start = tag_end + 1;
        let (body, next_cursor) = if self_closing {
            ("", body_start)
        } else {
            match fragment[body_start..].find("</invoke>") {
                Some(rel_close) => {
                    let close = body_start + rel_close;
                    (&fragment[body_start..close], close + "</invoke>".len())
                }
                None => {
                    if let Some((next_rel, _)) =
                        find_next_open_tag(&fragment[body_start..], &["invoke"])
                    {
                        let next = body_start + next_rel;
                        (&fragment[body_start..next], next)
                    } else {
                        (&fragment[body_start..], fragment.len())
                    }
                }
            }
        };

        if let Some(name) = invoke_name_from_tag_and_body(tag, body)
            && !name.trim().is_empty()
        {
            calls.push(XmlToolCall {
                name: name.trim().to_string(),
                arguments: parse_parameter_tags_lenient(body),
            });
        }
        cursor = next_cursor;
    }

    if calls.is_empty() { None } else { Some(calls) }
}

fn parse_parameter_tags_lenient(body: &str) -> Value {
    let mut map = serde_json::Map::new();
    let mut cursor = 0;
    while cursor < body.len() {
        let Some((rel_start, tag_name)) =
            find_next_open_tag(&body[cursor..], &["parameter", "param"])
        else {
            break;
        };
        let start = cursor + rel_start;
        let tag_body_start = start + tag_name.len() + 1;
        let Some(rel_tag_end) = body[tag_body_start..].find('>') else {
            break;
        };
        let tag_end = tag_body_start + rel_tag_end;
        let tag = &body[tag_body_start..tag_end];
        if tag.contains('<') {
            cursor = start + 1;
            continue;
        }
        let self_closing = tag.trim_end().ends_with('/');
        let value_start = tag_end + 1;
        let (raw_value, next_cursor) = if self_closing {
            ("", value_start)
        } else {
            match body[value_start..].find("</parameter>") {
                Some(rel_close) => {
                    let close = value_start + rel_close;
                    (&body[value_start..close], close + "</parameter>".len())
                }
                None => {
                    if let Some((next_rel, _)) =
                        find_next_open_tag(&body[value_start..], &["parameter", "param"])
                    {
                        let next = value_start + next_rel;
                        (&body[value_start..next], next)
                    } else {
                        (&body[value_start..], body.len())
                    }
                }
            }
        };

        if let Some(name) = attribute_alias_value(tag, &["name", "key"])
            && !name.trim().is_empty()
        {
            let raw_value = attribute_alias_value(tag, &["value", "content"])
                .unwrap_or_else(|| raw_value.trim().to_string());
            let raw_value = strip_cdata(raw_value.trim()).unwrap_or(raw_value.trim());
            let value = unescape_xml(raw_value);
            map.insert(name.trim().to_string(), parse_parameter_value(&value));
        }
        cursor = next_cursor;
    }
    Value::Object(map)
}

fn parse_parameter_value(raw: &str) -> Value {
    let raw = raw.trim();
    if raw.is_empty() {
        return Value::String(String::new());
    }
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_string()))
}

fn opening_tag_body<'a>(fragment: &'a str, tag: &str) -> Option<&'a str> {
    let start = find_open_tag(fragment, tag)?;
    let tag_body_start = start + tag.len() + 1;
    let tag_end = fragment[tag_body_start..].find('>')? + tag_body_start;
    Some(&fragment[tag_body_start..tag_end])
}

fn outer_tag_body<'a>(fragment: &'a str, tag: &str) -> Option<&'a str> {
    let start = find_open_tag(fragment, tag)?;
    let tag_body_start = start + tag.len() + 1;
    let body_start = fragment[tag_body_start..].find('>')? + tag_body_start + 1;
    let close_tag = format!("</{tag}>");
    let body_end = fragment[body_start..]
        .find(&close_tag)
        .map(|idx| body_start + idx)
        .unwrap_or(fragment.len());
    Some(&fragment[body_start..body_end])
}

fn invoke_name_from_tag_and_body(tag: &str, body: &str) -> Option<String> {
    attribute_alias_value(
        tag,
        &["name", "tool", "tool_name", "function", "function_name"],
    )
    .or_else(|| {
        tag_text_any(
            body,
            &["name", "tool", "tool_name", "function", "function_name"],
        )
    })
}

fn attribute_alias_value(tag: &str, aliases: &[&str]) -> Option<String> {
    aliases
        .iter()
        .find_map(|alias| xml_attribute_value(tag, alias))
}

fn node_attribute_alias<'a>(node: Node<'a, 'a>, aliases: &[&str]) -> Option<&'a str> {
    aliases.iter().find_map(|alias| node.attribute(*alias))
}

fn is_empty_object(value: &Value) -> bool {
    value.as_object().is_some_and(serde_json::Map::is_empty)
}

fn find_next_open_tag<'a>(text: &str, tags: &'a [&'a str]) -> Option<(usize, &'a str)> {
    tags.iter()
        .filter_map(|tag| find_open_tag(text, tag).map(|start| (start, *tag)))
        .min_by_key(|(start, _)| *start)
}

fn find_open_tag(text: &str, tag: &str) -> Option<usize> {
    let marker = format!("<{tag}");
    let mut offset = 0;
    while let Some(found) = text[offset..].find(&marker) {
        let start = offset + found;
        let after = start + marker.len();
        let next = text[after..].chars().next();
        if next.is_some_and(|ch| matches!(ch, '>' | '/' | ' ' | '\n' | '\r' | '\t')) {
            return Some(start);
        }
        offset = after;
    }
    None
}

fn find_close_tag_after(text: &str, tag: &str, from: usize) -> Option<usize> {
    let close = format!("</{tag}>");
    text[from..].find(&close).map(|idx| from + idx)
}

fn xml_attribute_value(tag: &str, attr: &str) -> Option<String> {
    let bytes = tag.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }

        let key_start = cursor;
        while cursor < bytes.len()
            && !bytes[cursor].is_ascii_whitespace()
            && bytes[cursor] != b'='
            && bytes[cursor] != b'/'
        {
            cursor += 1;
        }
        let key = &tag[key_start..cursor];

        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor >= bytes.len() || bytes[cursor] != b'=' {
            while cursor < bytes.len() && !bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            continue;
        }
        cursor += 1;

        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if cursor >= bytes.len() {
            break;
        }

        let quote = bytes[cursor];
        if quote != b'"' && quote != b'\'' {
            let value_start = cursor;
            while cursor < bytes.len()
                && !bytes[cursor].is_ascii_whitespace()
                && bytes[cursor] != b'/'
            {
                cursor += 1;
            }
            if key == attr {
                return Some(unescape_xml(&tag[value_start..cursor]));
            }
            continue;
        }
        cursor += 1;
        let value_start = cursor;
        while cursor < bytes.len() && bytes[cursor] != quote {
            cursor += 1;
        }
        if cursor >= bytes.len() {
            break;
        }
        let value = &tag[value_start..cursor];
        cursor += 1;

        if key == attr {
            return Some(unescape_xml(value));
        }
    }
    None
}

fn parse_lenient_tool_call(fragment: &str, tools: &[ToolDefinition]) -> Option<XmlToolCall> {
    if let Some(name) = tag_text_any(
        fragment,
        &["name", "tool", "tool_name", "function", "function_name"],
    ) {
        let arguments =
            parse_arguments_value(fragment).unwrap_or_else(|| simple_arguments(fragment));
        return Some(XmlToolCall { name, arguments });
    }

    let body = fragment
        .split_once('>')
        .map(|(_, body)| body)
        .unwrap_or(fragment);
    let body = body
        .rsplit_once("</tool_call>")
        .map(|(body, _)| body)
        .unwrap_or(body)
        .trim();
    if let Some(direct_tag) = first_tag_name(body) {
        if matches!(
            direct_tag.as_str(),
            "tool_call" | "name" | "arguments" | "tool_result" | "function_calls"
        ) {
            return None;
        }
        return Some(XmlToolCall {
            name: direct_tag,
            arguments: simple_arguments(body),
        });
    }

    let text = unescape_xml(body).trim().to_string();
    if text.is_empty() {
        return None;
    }
    if tools.iter().any(|tool| tool.name == text) {
        return Some(XmlToolCall {
            name: text,
            arguments: json!({}),
        });
    }
    if let Some(skill_tool) = tools.iter().find(|tool| tool.name == "Skill") {
        let arg_name = preferred_argument_name(skill_tool, &["skill", "name", "id", "skill_name"]);
        return Some(XmlToolCall {
            name: "Skill".to_string(),
            arguments: one_argument(arg_name, text),
        });
    }
    Some(XmlToolCall {
        name: text,
        arguments: json!({}),
    })
}

/// Apply minimal, conservative cleanups to a parsed call: rename common
/// argument aliases (e.g. `cmd` → `command`) so the call shape matches the
/// declared tool. Aggressive guessing (renaming an unknown tool into
/// `Skill`, inferring tool intent from argument text) is intentionally NOT
/// done here — the proxy must not silently rewrite the model's intent into a
/// different tool.
fn normalize_tool_call(mut call: XmlToolCall, tools: &[ToolDefinition]) -> XmlToolCall {
    if let Some(tool) = tools.iter().find(|tool| tool.name == call.name) {
        coerce_argument_aliases(&mut call.arguments, tool);
    }
    call
}

fn coerce_argument_aliases(arguments: &mut Value, tool: &ToolDefinition) {
    let Some(map) = arguments.as_object_mut() else {
        return;
    };
    let Some(properties) = tool.parameters.get("properties").and_then(Value::as_object) else {
        return;
    };

    for aliases in [
        &["command", "cmd", "script"][..],
        &["cmd", "command", "script"][..],
        &["skill", "name", "id", "skill_name"][..],
        &["name", "skill", "id", "skill_name"][..],
        &["path", "file", "filepath", "file_path"][..],
        &["query", "q", "pattern"][..],
    ] {
        let canonical = aliases[0];
        if !properties.contains_key(canonical) || map.contains_key(canonical) {
            continue;
        }
        // If any alias is itself a declared property the author meant the
        // names to coexist as distinct fields; do NOT collapse them.
        if aliases
            .iter()
            .skip(1)
            .any(|alias| properties.contains_key(*alias))
        {
            continue;
        }
        if let Some((_, value)) = aliases
            .iter()
            .skip(1)
            .find_map(|alias| map.get(*alias).cloned().map(|value| (*alias, value)))
        {
            map.insert(canonical.to_string(), value);
        }
    }
}

fn one_argument(name: String, value: String) -> Value {
    let mut map = serde_json::Map::new();
    map.insert(name, Value::String(value));
    Value::Object(map)
}

fn preferred_argument_name(tool: &ToolDefinition, preferred: &[&str]) -> String {
    for name in preferred {
        if tool
            .parameters
            .get("properties")
            .and_then(Value::as_object)
            .is_some_and(|properties| properties.contains_key(*name))
        {
            return (*name).to_string();
        }
    }
    if let Some(required) = tool.parameters.get("required").and_then(Value::as_array)
        && let Some(name) = required.first().and_then(Value::as_str)
    {
        return name.to_string();
    }
    "skill".to_string()
}

fn first_tag_name(text: &str) -> Option<String> {
    let start = text.find('<')? + 1;
    let rest = text[start..].trim_start();
    if rest.starts_with('/') || rest.starts_with('!') || rest.starts_with('?') {
        return None;
    }
    let end = rest
        .find(|ch: char| ch == '>' || ch == '/' || ch.is_whitespace())
        .unwrap_or(rest.len());
    let name = rest[..end].trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

fn simple_arguments(fragment: &str) -> Value {
    let mut map = serde_json::Map::new();
    let mut rest = fragment;
    while let Some(open) = rest.find('<') {
        rest = &rest[open + 1..];
        if rest.starts_with('/') || rest.starts_with('!') || rest.starts_with('?') {
            continue;
        }
        let Some(close) = rest.find('>') else {
            break;
        };
        let tag = rest[..close].trim();
        let tag = tag
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_end_matches('/');
        rest = &rest[close + 1..];
        if tag.is_empty()
            || matches!(
                tag,
                "tool_call"
                    | "name"
                    | "arguments"
                    | "function_calls"
                    | "invoke"
                    | "parameter"
                    | "param"
            )
            || tag.chars().next().is_some_and(char::is_uppercase)
        {
            continue;
        }
        let close_tag = format!("</{tag}>");
        let Some(value_end) = rest.find(&close_tag) else {
            continue;
        };
        let value = unescape_xml(rest[..value_end].trim());
        map.insert(tag.to_string(), Value::String(value));
        rest = &rest[value_end + close_tag.len()..];
    }
    Value::Object(map)
}

fn parse_arguments_value(fragment: &str) -> Option<Value> {
    tag_text_any(fragment, &["arguments", "args", "input", "parameters"])
        .or_else(|| tag_body_lenient_any(fragment, &["arguments", "args", "input", "parameters"]))
        .map(|raw| parse_arguments_text(&raw))
}

fn parse_arguments_text(raw: &str) -> Value {
    let raw = strip_cdata_lenient(raw.trim()).trim();
    if raw.is_empty() {
        return json!({});
    }
    serde_json::from_str(raw)
        .ok()
        .or_else(|| parse_json_object_lenient(raw))
        .unwrap_or_else(|| Value::String(raw.to_string()))
}

fn parse_json_object_lenient(raw: &str) -> Option<Value> {
    let mut inner = raw.trim();
    inner = inner.strip_prefix('{').unwrap_or(inner).trim();
    inner = inner.strip_suffix('}').unwrap_or(inner).trim();

    let mut map = serde_json::Map::new();
    let mut cursor = 0;
    while let Some((key, colon)) = find_json_key(inner, cursor) {
        let mut value_start = colon + 1;
        while value_start < inner.len() && inner.as_bytes()[value_start].is_ascii_whitespace() {
            value_start += 1;
        }

        let quoted = inner.as_bytes().get(value_start) == Some(&b'"');
        if quoted {
            value_start += 1;
        }

        let delimiter = find_next_json_pair_delimiter(inner, value_start);
        let value_end = delimiter.unwrap_or(inner.len());
        let raw_value = inner[value_start..value_end].trim();
        let value = if quoted {
            Value::String(clean_lenient_string(raw_value))
        } else {
            serde_json::from_str(raw_value).unwrap_or_else(|_| {
                Value::String(raw_value.trim_end_matches(',').trim().to_string())
            })
        };
        map.insert(key, value);

        let Some(next) = delimiter else {
            break;
        };
        cursor = next + 1;
    }

    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

fn find_json_key(text: &str, from: usize) -> Option<(String, usize)> {
    let mut cursor = from;
    while cursor < text.len() {
        let rel_start = text[cursor..].find('"')?;
        let key_start = cursor + rel_start + 1;
        let rel_end = text[key_start..].find('"')?;
        let key_end = key_start + rel_end;
        let mut colon = key_end + 1;
        while colon < text.len() && text.as_bytes()[colon].is_ascii_whitespace() {
            colon += 1;
        }
        if text.as_bytes().get(colon) == Some(&b':') {
            return Some((text[key_start..key_end].to_string(), colon));
        }
        cursor = key_end + 1;
    }
    None
}

fn find_next_json_pair_delimiter(text: &str, from: usize) -> Option<usize> {
    let mut cursor = from;
    while cursor < text.len() {
        let rel_comma = text[cursor..].find(',')?;
        let comma = cursor + rel_comma;
        let mut after = comma + 1;
        while after < text.len() && text.as_bytes()[after].is_ascii_whitespace() {
            after += 1;
        }
        if text.as_bytes().get(after) != Some(&b'"') {
            cursor = comma + 1;
            continue;
        }
        let key_start = after + 1;
        let rel_key_end = text[key_start..].find('"')?;
        let key_end = key_start + rel_key_end;
        let mut colon = key_end + 1;
        while colon < text.len() && text.as_bytes()[colon].is_ascii_whitespace() {
            colon += 1;
        }
        if text.as_bytes().get(colon) == Some(&b':') {
            return Some(comma);
        }
        cursor = comma + 1;
    }
    None
}

fn clean_lenient_string(raw: &str) -> String {
    let mut value = raw.trim();
    if let Some(stripped) = value.strip_suffix('"') {
        value = stripped;
    }
    unescape_jsonish_string(value.trim())
}

fn unescape_jsonish_string(value: &str) -> String {
    value
        .replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\t", "\t")
        .replace("\\\"", "\"")
        .replace("\\\\", "\\")
}

fn tag_body_lenient_any(fragment: &str, tags: &[&str]) -> Option<String> {
    tags.iter().find_map(|tag| tag_body_lenient(fragment, tag))
}

fn tag_body_lenient(fragment: &str, tag: &str) -> Option<String> {
    let start = find_open_tag(fragment, tag)?;
    let tag_body_start = start + tag.len() + 1;
    let tag_end = fragment[tag_body_start..].find('>')? + tag_body_start;
    if fragment[tag_body_start..tag_end].contains('<') {
        return None;
    }
    let body_start = tag_end + 1;
    let close_tag = format!("</{tag}>");
    let body_end = fragment[body_start..]
        .find(&close_tag)
        .or_else(|| fragment[body_start..].find("</tool_call>"))
        .map(|idx| body_start + idx)
        .unwrap_or(fragment.len());
    Some(unescape_xml(fragment[body_start..body_end].trim()))
}

fn tag_text_any(fragment: &str, tags: &[&str]) -> Option<String> {
    tags.iter().find_map(|tag| tag_text(fragment, tag))
}

fn tag_text(fragment: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = fragment.find(&open)? + open.len();
    let end = fragment[start..].find(&close)? + start;
    let raw = fragment[start..end].trim();
    // Models are instructed to wrap argument bodies in CDATA. If the wrapper
    // survived (because strict parsing failed for some other reason), strip
    // it here so the inner JSON parses.
    if let Some(inner) = strip_cdata(raw) {
        return Some(inner.to_string());
    }
    Some(unescape_xml(raw))
}

fn strip_cdata(raw: &str) -> Option<&str> {
    raw.strip_prefix("<![CDATA[")
        .and_then(|s| s.strip_suffix("]]>"))
}

fn strip_cdata_lenient(raw: &str) -> &str {
    let Some(stripped) = raw.strip_prefix("<![CDATA[") else {
        return raw;
    };
    stripped.strip_suffix("]]>").unwrap_or(stripped)
}

pub fn build_tool_call(name: &str, arguments: Value) -> String {
    let args = serde_json::to_string(&arguments).unwrap_or_else(|_| "{}".to_string());
    format!(
        "<tool_call>\n  <name>{}</name>\n  <arguments><![CDATA[{}]]></arguments>\n</tool_call>",
        escape_xml(name),
        escape_cdata(&args),
    )
}

pub fn build_tool_result(name: &str, ok: bool, content: Value) -> String {
    let body = json!({
        "ok": ok,
        "content": content,
    });
    let body_text = serde_json::to_string(&body).unwrap_or_else(|_| "{\"ok\":false}".to_string());
    format!(
        "<tool_result>\n  <name>{}</name>\n  <content><![CDATA[{}]]></content>\n</tool_result>",
        escape_xml(name),
        escape_cdata(&body_text),
    )
}

fn child_text<'a>(node: roxmltree::Node<'a, 'a>, tag: &str) -> Option<&'a str> {
    node.children()
        .find(|child| child.is_element() && child.tag_name().name() == tag)
        .and_then(|child| child.text())
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn unescape_xml(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Escape any `]]>` sequence that would otherwise terminate the surrounding
/// `<![CDATA[ ... ]]>` section. The standard trick is to split the section so
/// the literal close sequence never appears verbatim inside one section.
fn escape_cdata(content: &str) -> String {
    content.replace("]]>", "]]]]><![CDATA[>")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skill_tool() -> ToolDefinition {
        ToolDefinition {
            name: "Skill".to_string(),
            description: "Use a skill".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {"skill": {"type": "string"}},
                "required": ["skill"]
            }),
        }
    }

    fn bash_tool() -> ToolDefinition {
        ToolDefinition {
            name: "Bash".to_string(),
            description: "Run shell command".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {"command": {"type": "string"}},
                "required": ["command"]
            }),
        }
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
    fn parses_cdata_arguments() {
        let calls = parse_tool_calls(
            r#"<tool_call>
  <name>math_eval</name>
  <arguments><![CDATA[{"expression":"1 + 2"}]]></arguments>
</tool_call>"#,
            &[],
        );

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "math_eval");
        assert_eq!(calls[0].arguments["expression"], "1 + 2");
    }

    #[test]
    fn parses_multiple_tool_calls_in_order() {
        let calls = parse_tool_calls(
            r#"sure, I'll do both.
<tool_call>
  <name>Read</name>
  <arguments><![CDATA[{"path":"a"}]]></arguments>
</tool_call>
<tool_call>
  <name>Read</name>
  <arguments><![CDATA[{"path":"b"}]]></arguments>
</tool_call>"#,
            &[],
        );

        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].arguments["path"], "a");
        assert_eq!(calls[1].arguments["path"], "b");
    }

    #[test]
    fn parses_function_calls_invoke_blocks_in_order() {
        let calls = parse_tool_calls(
            r#"Thought for 18s, read 1 file.
<tool_call>
  <function_calls>
    <invoke name="Read">
      <parameter name="file_path">/Users/starshine/Documents/Workspace/llm-tool-whisper/README.md</parameter>
    </invoke>
    <invoke name="Read">
      <parameter name="file_path">/Users/starshine/Documents/Workspace/llm-tool-whisper/Dockerfile</parameter>
    </invoke>
    <invoke name="Read">
      <parameter name="file_path">/Users/starshine/Documents/Workspace/llm-tool-whisper/docker-compose.yml</parameter>
    </invoke>
    <invoke name="Read">
      <parameter name="file_path">/Users/starshine/Documents/Workspace/llm-tool-whisper/Cargo.toml</parameter>
    </invoke>
  </function_calls>
</tool_call>"#,
            &[read_tool()],
        );

        assert_eq!(calls.len(), 4);
        assert!(calls.iter().all(|call| call.name == "Read"));
        assert_eq!(
            calls[0].arguments["file_path"],
            "/Users/starshine/Documents/Workspace/llm-tool-whisper/README.md"
        );
        assert_eq!(
            calls[3].arguments["file_path"],
            "/Users/starshine/Documents/Workspace/llm-tool-whisper/Cargo.toml"
        );
    }

    #[test]
    fn parses_function_call_parameter_json_values() {
        let calls = parse_tool_calls(
            r#"<tool_call>
  <function_calls>
    <invoke name="Batch">
      <parameter name="items">["README.md","Cargo.toml"]</parameter>
      <parameter name="dry_run">true</parameter>
    </invoke>
  </function_calls>
</tool_call>"#,
            &[],
        );

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Batch");
        assert_eq!(
            calls[0].arguments["items"],
            json!(["README.md", "Cargo.toml"])
        );
        assert_eq!(calls[0].arguments["dry_run"], true);
    }

    #[test]
    fn parses_function_calls_with_unescaped_parameter_text() {
        let calls = parse_tool_calls(
            r#"<tool_call>
  <function_calls>
    <invoke name="Search">
      <parameter name="query">Cargo & Dockerfile</parameter>
    </invoke>
  </function_calls>
</tool_call>"#,
            &[],
        );

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Search");
        assert_eq!(calls[0].arguments["query"], "Cargo & Dockerfile");
    }

    #[test]
    fn parses_bare_function_calls_without_tool_call_wrapper() {
        let calls = parse_tool_calls(
            r#"I'll read it now.
<function_calls>
  <invoke name="Read">
    <parameter name="file_path">README.md</parameter>
  </invoke>
</function_calls>"#,
            &[read_tool()],
        );

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Read");
        assert_eq!(calls[0].arguments["file_path"], "README.md");
    }

    #[test]
    fn parses_bare_invoke_without_wrappers() {
        let calls = parse_tool_calls(
            r#"<invoke name="Read">
  <parameter name="file_path">README.md</parameter>
</invoke>"#,
            &[read_tool()],
        );

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Read");
        assert_eq!(calls[0].arguments["file_path"], "README.md");
    }

    #[test]
    fn parses_incomplete_tool_call_at_end_of_text() {
        let calls = parse_tool_calls(
            r#"<tool_call>
  <function_calls>
    <invoke name=Read>
      <parameter name=file_path>README.md"#,
            &[read_tool()],
        );

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Read");
        assert_eq!(calls[0].arguments["file_path"], "README.md");
    }

    #[test]
    fn parses_tool_call_name_attribute_with_parameter_value_attribute() {
        let calls = parse_tool_calls(
            r#"<tool_call name=Read>
  <parameter name=file_path value="README.md" />
</tool_call>"#,
            &[read_tool()],
        );

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Read");
        assert_eq!(calls[0].arguments["file_path"], "README.md");
    }

    #[test]
    fn parses_orphan_name_arguments_blocks_without_opening_tool_call() {
        let calls = parse_tool_calls(
            r#"<name>Read</name>
    <arguments><![CDATA[{"file_path": "/Users/starshine/Documents/Workspace/llm-tool-whisper/Dockerfile"}
  </tool_call>
    <name>Read</name>
    <arguments><![CDATA[{"file_path": "/Users/starshine/Documents/Workspace/llm-tool-whisper/docker-compose.yml"}
  </tool_call>
    <name>Bash</name>
    <arguments><![CDATA[{"command": "ls -la /Users/starshine/Documents/Workspace/llm-tool-whisper/.github/workflows/ 2>/dev/null || echo "NO_WORKFLOWS_DIR"", "description": "Check for existing GitHub
  workflows"}
  </tool_call>
    <name>Bash</name>
    <arguments><![CDATA[{"command": "cd /Users/starshine/Documents/Workspace/llm-tool-whisper && git remote -v", "description": "Check git remote URL for repo name"}
  </tool_call>"#,
            &[read_tool(), bash_tool()],
        );

        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0].name, "Read");
        assert_eq!(
            calls[0].arguments["file_path"],
            "/Users/starshine/Documents/Workspace/llm-tool-whisper/Dockerfile"
        );
        assert_eq!(
            calls[1].arguments["file_path"],
            "/Users/starshine/Documents/Workspace/llm-tool-whisper/docker-compose.yml"
        );
        assert_eq!(calls[2].name, "Bash");
        assert_eq!(
            calls[2].arguments["command"],
            "ls -la /Users/starshine/Documents/Workspace/llm-tool-whisper/.github/workflows/ 2>/dev/null || echo \"NO_WORKFLOWS_DIR\""
        );
        assert_eq!(
            calls[2].arguments["description"],
            "Check for existing GitHub\n  workflows"
        );
        assert_eq!(
            calls[3].arguments["command"],
            "cd /Users/starshine/Documents/Workspace/llm-tool-whisper && git remote -v"
        );
    }

    #[test]
    fn strips_orphan_name_arguments_blocks_from_visible_text() {
        let visible = remove_tool_call_fragments(
            r#"pre <name>Read</name>
    <arguments><![CDATA[{"file_path": "Dockerfile"}
  </tool_call> post"#,
        );

        assert_eq!(visible, "pre  post");
    }

    #[test]
    fn strips_bare_function_calls_from_visible_text() {
        let visible = remove_tool_call_fragments(
            r#"pre <function_calls>
  <invoke name="Read">
    <parameter name="file_path">README.md</parameter>
  </invoke>
</function_calls> post"#,
        );

        assert_eq!(visible, "pre  post");
    }

    #[test]
    fn invalid_pseudo_invoke_does_not_hide_later_valid_call() {
        let calls = parse_tool_calls(
            r#"literal <invoke maybe prose
<tool_call>
  <name>Read</name>
  <arguments><![CDATA[{"file_path":"README.md"}]]></arguments>
</tool_call>"#,
            &[read_tool()],
        );

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Read");
        assert_eq!(calls[0].arguments["file_path"], "README.md");
    }

    #[test]
    fn parses_bare_skill_name_as_skill_tool_call() {
        let calls = parse_tool_calls("<tool_call> system-info </tool_call>", &[skill_tool()]);

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Skill");
        assert_eq!(calls[0].arguments["skill"], "system-info");
    }

    #[test]
    fn parses_tool_name_tag_as_lenient_tool_call() {
        let calls = parse_tool_calls(
            r#"<tool_call>
  <Skill>
    <skill>system-info</skill>
  </Skill>
</tool_call>"#,
            &[skill_tool()],
        );

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Skill");
        assert_eq!(calls[0].arguments["skill"], "system-info");
    }

    #[test]
    fn coerces_bash_cmd_alias_to_required_command() {
        let calls = parse_tool_calls(
            r#"<tool_call>
  <name>Bash</name>
  <arguments><![CDATA[{"cmd":"date"}]]></arguments>
</tool_call>"#,
            &[bash_tool(), skill_tool()],
        );

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Bash");
        assert_eq!(calls[0].arguments["command"], "date");
    }

    #[test]
    fn does_not_rewrite_unknown_tool_into_skill() {
        // A clearly bogus call must be returned as-is so the CLIENT can reject
        // it. The proxy must not silently rename it into `Skill` just because
        // `Skill` happens to be configured.
        let calls = parse_tool_calls(
            r#"<tool_call>
  <name>Bash</name>
  <arguments><![CDATA[{"description":"Get system info"}]]></arguments>
</tool_call>"#,
            &[bash_tool(), skill_tool()],
        );
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "Bash");
        assert_eq!(calls[0].arguments["description"], "Get system info");
    }

    #[test]
    fn build_tool_call_escapes_cdata_close_sequence() {
        let block = build_tool_call("answer", json!({"text": "raw ]]> end"}));
        // No raw `]]>` may appear without being immediately followed by
        // `<![CDATA[`, otherwise the section closes early.
        let payload_start = block.find("<![CDATA[").unwrap() + "<![CDATA[".len();
        let inside_end = block.rfind("]]></arguments>").unwrap();
        let inside = &block[payload_start..inside_end];
        let mut idx = 0;
        while let Some(found) = inside[idx..].find("]]>") {
            let abs = idx + found;
            let after = abs + "]]>".len();
            assert!(
                inside[after..].starts_with("<![CDATA["),
                "raw `]]>` at byte {abs} not followed by `<![CDATA[`: {inside:?}",
            );
            idx = after;
        }
    }

    #[test]
    fn build_tool_result_escapes_cdata_close_sequence() {
        let block = build_tool_result("echo", true, json!({"raw": "before ]]> after"}));
        // Same invariant as above: every raw `]]>` must be a split, not a
        // genuine close, anywhere inside the content payload.
        let payload_start = block.find("<![CDATA[").unwrap() + "<![CDATA[".len();
        let inside_end = block.rfind("]]></content>").unwrap();
        let inside = &block[payload_start..inside_end];
        let mut idx = 0;
        while let Some(found) = inside[idx..].find("]]>") {
            let abs = idx + found;
            let after = abs + "]]>".len();
            assert!(
                inside[after..].starts_with("<![CDATA["),
                "raw `]]>` at byte {abs} not followed by `<![CDATA[`: {inside:?}",
            );
            idx = after;
        }
    }

    #[test]
    fn round_trips_cdata_payload_containing_close_sequence() {
        // A built tool_call carrying `]]>` must parse back to the same JSON.
        let block = build_tool_call("echo", json!({"text": "raw ]]> end"}));
        let calls = parse_tool_calls(&block, &[]);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "echo");
        assert_eq!(calls[0].arguments["text"], "raw ]]> end");
    }

    #[test]
    fn coerce_alias_skips_when_alias_is_distinct_property() {
        // A tool genuinely declaring both `command` and `cmd` as distinct
        // fields must keep them distinct: an arg of `cmd` must NOT bleed into
        // `command`.
        let tool = ToolDefinition {
            name: "Both".to_string(),
            description: "Has both fields".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "command": {"type": "string"},
                    "cmd": {"type": "string"},
                },
                "required": ["command"]
            }),
        };
        let calls = parse_tool_calls(
            r#"<tool_call>
  <name>Both</name>
  <arguments><![CDATA[{"cmd":"date"}]]></arguments>
</tool_call>"#,
            &[tool],
        );
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].arguments.get("command"), None);
        assert_eq!(calls[0].arguments["cmd"], "date");
    }
}
