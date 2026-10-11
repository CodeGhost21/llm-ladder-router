//! Surplus's charge, written where `OpenAI`-dialect clients read a cost.
//!
//! Clients that budget a run read `usage.cost`, the field `OpenRouter` fills
//! with what a call charged. Surplus never fills it with its own charge:
//! measured 2026-10-12, a buffered chat answer carries `cost: 0` beside
//! `is_byok: true` (the seller's upstream bill, not the buyer's), and a
//! streamed one carries no `cost` at all. What Surplus charged travels as
//! `usage.buyer_cost_micro` in the body, or in the `x-si-buyer-cost-micro`
//! header when a buffered body has none. A budget ledger reading `cost` sees
//! every call as free, and an agent loop capped at twenty dollars is not
//! capped at all.
//!
//! So on Surplus's chat answers the router restates that charge, in dollars,
//! as `usage.cost`. Nothing else in the body changes, and a body or event that
//! names no charge is relayed byte for byte.

/// Sets `usage.cost` to Surplus's own charge, in dollars.
///
/// The charge is `usage.buyer_cost_micro`, else the header's figure. Returns
/// whether anything was written; a usage object naming no charge, or a
/// negative or non-finite one, is left alone.
pub fn stamp_usage(usage: &mut serde_json::Value, header_micro: Option<f64>) -> bool {
    let Some(object) = usage.as_object_mut() else {
        return false;
    };
    let Some(micro) = object
        .get("buyer_cost_micro")
        .and_then(serde_json::Value::as_f64)
        .or(header_micro)
        .filter(|micro| micro.is_finite() && *micro >= 0.0)
    else {
        return false;
    };
    object.insert("cost".to_string(), (micro / 1_000_000.0).into());
    true
}

/// A buffered chat answer with its charge stamped, or `None` when the body is
/// not JSON or names no charge and should be relayed as it came.
#[must_use]
pub fn stamp_body(body: &[u8], header_micro: Option<f64>) -> Option<Vec<u8>> {
    let mut answer = serde_json::from_slice::<serde_json::Value>(body).ok()?;
    let usage = answer.get_mut("usage")?;
    if !stamp_usage(usage, header_micro) {
        return None;
    }
    serde_json::to_vec(&answer).ok()
}

/// Stamps the charge into a chat event stream as it passes.
///
/// Bytes are released line by line: a complete line is relayed at once, and
/// only the `data:` line carrying a charge is re-serialized. A partial line is
/// held until its newline arrives, which costs a caller nothing it could have
/// used — no client acts on half an event.
#[derive(Debug, Default)]
pub struct SseStamper {
    line: Vec<u8>,
}

impl SseStamper {
    /// Takes the next chunk of the stream and returns the bytes ready to relay.
    pub fn feed(&mut self, chunk: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(chunk.len());
        let mut rest = chunk;
        while let Some(end) = rest.iter().position(|byte| *byte == b'\n') {
            self.line.extend_from_slice(&rest[..=end]);
            out.extend_from_slice(&stamp_line(&self.line));
            self.line.clear();
            rest = &rest[end + 1..];
        }
        self.line.extend_from_slice(rest);
        out
    }

    /// Releases whatever the stream ended on without a final newline.
    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        stamp_line(&self.line)
    }
}

/// One SSE line, its charge stamped when it is a `data:` event carrying one.
fn stamp_line(line: &[u8]) -> Vec<u8> {
    let body_end = line
        .iter()
        .rposition(|byte| !matches!(byte, b'\n' | b'\r'))
        .map_or(0, |last| last + 1);
    let (content, ending) = line.split_at(body_end);
    let Some(data) = content.strip_prefix(b"data:") else {
        return line.to_vec();
    };
    // Cheap test first: only the usage event names a charge, and parsing every
    // token's event to find it would be the expensive way to relay nothing.
    if !data.windows(16).any(|window| window == b"buyer_cost_micro") {
        return line.to_vec();
    }
    let Some(stamped) = stamp_body(data.trim_ascii(), None) else {
        return line.to_vec();
    };
    let mut out = b"data: ".to_vec();
    out.extend_from_slice(&stamped);
    out.extend_from_slice(ending);
    out
}

#[cfg(test)]
#[path = "cost_test.rs"]
mod test;
