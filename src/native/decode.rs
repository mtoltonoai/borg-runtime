//! Enforce structural admission while building JSON, before an oversized tree
//! exists. Serde remains the JSON syntax/number/string parser.
use crate::limits::NativeLimits;
use super::format::FormatIssue;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::{
    fmt,
    io::{self, Read},
};
use tokio::time::Instant;

struct Work {
    remaining: usize,
    depth_limit: usize,
    limit: usize,
    deadline: Instant,
    failure: Option<&'static str>,
}
impl Work {
    fn admit<E: de::Error>(&mut self, depth: usize) -> Result<(), E> {
        let error = if Instant::now() >= self.deadline {
            Some("native_deadline_exceeded")
        } else if self.remaining == 0 {
            Some("native_json_complexity_bound_native_json_nodes")
        } else if depth > self.depth_limit {
            Some("native_json_complexity_bound_native_json_depth")
        } else {
            None
        };
        if let Some(error) = error {
            self.failure = Some(error);
            return Err(E::custom("bounded JSON admission"));
        }
        self.remaining -= 1;
        Ok(())
    }
}

struct Node<'a> {
    work: &'a mut Work,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for Node<'_> {
    type Value = Value;
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<Value, D::Error> {
        self.work.admit(self.depth)?;
        decoder.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Node<'_> {
    type Value = Value;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded JSON")
    }
    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
        Ok(Value::Number(value.into()))
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("nonfinite JSON number"))
    }
    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
        if value.len() > self.work.limit {
            self.work.failure = Some("native_json_byte_bound");
            return Err(E::custom("bounded JSON string"));
        }
        Ok(Value::String(value.to_owned()))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut input: A) -> Result<Value, A::Error> {
        let mut values = Vec::new(); // Never reserve from untrusted size_hint.
        while let Some(value) = input.next_element_seed(Node {
            work: self.work,
            depth: self.depth + 1,
        })? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut input: A) -> Result<Value, A::Error> {
        let mut values = Map::new();
        while let Some(key) = input.next_key_seed(Key {
            work: self.work,
            depth: self.depth + 1,
        })? {
            if values.contains_key(&key) {
                self.work.failure = Some("native_json_duplicate_key");
                return Err(de::Error::custom("duplicate JSON key"));
            }
            let value = input.next_value_seed(Node {
                work: self.work,
                depth: self.depth + 1,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}
struct Key<'a> {
    work: &'a mut Work,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for Key<'_> {
    type Value = String;
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<String, D::Error> {
        self.work.admit(self.depth)?;
        decoder.deserialize_str(self)
    }
}
impl Visitor<'_> for Key<'_> {
    type Value = String;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bounded JSON object key")
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<String, E> {
        if value.len() > self.work.limit {
            self.work.failure = Some("native_json_byte_bound");
            return Err(E::custom("bounded JSON key"));
        }
        Ok(value.to_owned())
    }
}

// Native component scope: byte/node/depth admission during native JSON construction, including scalar deadline checks.
pub(super) fn parse(
    bytes: &[u8],
    limit: usize,
    deadline: Instant,
    malformed: &'static str,
    policy: &NativeLimits,
    field: &str,
) -> Result<Value, String> {
    parse_inner(bytes, limit, deadline, policy, field)
        .map_err(|error| error.historical(malformed))
}

/// Only syntax/EOF and duplicate-key failures become format observations.
/// Admission, clock and parser infrastructure errors never authorize repair.
pub(super) fn parse_final(
    bytes: &[u8],
    limit: usize,
    deadline: Instant,
    policy: &NativeLimits,
    field: &str,
) -> Result<Result<Value, FormatIssue>, String> {
    match parse_inner(bytes, limit, deadline, policy, field) {
        Ok(value) => Ok(Ok(value)),
        Err(ParseFailure::DuplicateKey) => Ok(Err(FormatIssue::DuplicateKey {})),
        Err(ParseFailure::Json(error)) if matches!(
            error.classify(), serde_json::error::Category::Syntax | serde_json::error::Category::Eof
        ) => {
            let line = u64::try_from(error.line()).map_err(|_| "native_json_location_invalid")?;
            let column = u64::try_from(error.column()).map_err(|_| "native_json_location_invalid")?;
            Ok(Err(FormatIssue::InvalidJson { line, column }))
        }
        Err(error) => Err(error.historical("native_output_not_json")),
    }
}

// The foreign error is held only long enough to read its category and numeric
// location. It is never formatted, serialized or included in feedback.
enum ParseFailure {
    Policy(String),
    DuplicateKey,
    Json(serde_json::Error),
}
impl ParseFailure {
    fn historical(self, malformed: &'static str) -> String {
        match self {
            Self::Policy(error) => error,
            Self::DuplicateKey => "native_json_duplicate_key".into(),
            Self::Json(_) => malformed.into(),
        }
    }
}

fn parse_inner(
    bytes: &[u8],
    limit: usize,
    deadline: Instant,
    policy: &NativeLimits,
    field: &str,
) -> Result<Value, ParseFailure> {
    if policy.decode_check_bytes == 0 {
        return Err(ParseFailure::Policy("native_json_configuration_invalid_native_decode_check_bytes".into()));
    }
    if bytes.len() > limit {
        return Err(ParseFailure::Policy(format!("native_json_byte_bound_{field}")));
    }
    let mut work = Work {
        remaining: policy.json_nodes,
        depth_limit: policy.json_depth,
        limit,
        deadline,
        failure: None,
    };
    // from_reader invokes this at the byte boundary, including inside one long
    // string/number. The configured byte cadence avoids an uninterruptible
    // complete scalar parse while retaining Serde syntax/number semantics.
    let reader = DeadlineReader {
        bytes,
        deadline,
        check_bytes: policy.decode_check_bytes,
        since_check: 0,
        expired: false,
    };
    let mut decoder = serde_json::Deserializer::from_reader(reader);
    let result = Node {
        work: &mut work,
        depth: 0,
    }
    .deserialize(&mut decoder);
    let value = result.map_err(|error| {
        if Instant::now() >= deadline {
            ParseFailure::Policy("native_deadline_exceeded".into())
        } else {
            match work.failure {
                Some("native_json_byte_bound") => ParseFailure::Policy(format!("native_json_byte_bound_{field}")),
                Some("native_json_duplicate_key") => ParseFailure::DuplicateKey,
                Some(code) => ParseFailure::Policy(code.into()),
                None => ParseFailure::Json(error),
            }
        }
    })?;
    end_inner(&mut decoder, deadline)?;
    Ok(value)
}

fn end_inner<'de, R: serde_json::de::Read<'de>>(
    decoder: &mut serde_json::Deserializer<R>,
    deadline: Instant,
) -> Result<(), ParseFailure> {
    decoder.end().map_err(|error| {
        if Instant::now() >= deadline {
            ParseFailure::Policy("native_deadline_exceeded".into())
        } else {
            ParseFailure::Json(error)
        }
    })
}

#[cfg(test)]
fn end_checked<'de, R: serde_json::de::Read<'de>>(
    decoder: &mut serde_json::Deserializer<R>,
    deadline: Instant,
    malformed: &'static str,
) -> Result<(), String> {
    end_inner(decoder, deadline).map_err(|error| error.historical(malformed))
}

struct DeadlineReader<'a> {
    bytes: &'a [u8],
    deadline: Instant,
    check_bytes: usize,
    since_check: usize,
    expired: bool,
}
impl Read for DeadlineReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.since_check == 0 && Instant::now() >= self.deadline {
            self.expired = true;
        }
        if self.expired {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "deadline"));
        }
        let size = output
            .len()
            .min(self.bytes.len())
            .min(self.check_bytes - self.since_check);
        output[..size].copy_from_slice(&self.bytes[..size]);
        self.bytes = &self.bytes[size..];
        self.since_check = (self.since_check + size) % self.check_bytes;
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn policy() -> NativeLimits {
        serde_json::from_str(include_str!("../../limits-native-seed.json")).unwrap()
    }

    fn parse_fixture(bytes: &[u8], policy: &NativeLimits) -> Result<Value, String> {
        parse(
            bytes,
            bytes.len(),
            Instant::now() + Duration::from_secs(5),
            "malformed",
            policy,
            "test_bytes",
        )
    }

    #[test]
    fn byte_refusal_identifies_the_supplied_field() {
        assert_eq!(
            parse(
                b"[0]",
                2,
                Instant::now() + Duration::from_secs(5),
                "malformed",
                &policy(),
                "test_bytes",
            )
            .unwrap_err(),
            "native_json_byte_bound_test_bytes"
        );
    }

    #[test]
    // Native component scope: metadata can raise or lower admission depth.
    fn json_depth_uses_metadata_above_and_below_the_seed_limit() {
        let seed = policy();
        let depth = seed.json_depth + 1;
        let expected = (0..depth).fold(Value::Null, |value, _| Value::Array(vec![value]));
        let bytes = serde_json::to_vec(&expected).unwrap();
        let mut raised = seed.clone();
        raised.json_depth = depth;

        assert_eq!(parse_fixture(&bytes, &raised).unwrap(), expected);
        assert_eq!(
            parse_fixture(&bytes, &seed).unwrap_err(),
            "native_json_complexity_bound_native_json_depth"
        );

        let mut lowered = seed.clone();
        lowered.json_depth = 1;
        assert_eq!(
            parse_fixture(b"[[0]]", &seed).unwrap(),
            serde_json::json!([[0]])
        );
        assert_eq!(
            parse_fixture(b"[[0]]", &lowered).unwrap_err(),
            "native_json_complexity_bound_native_json_depth"
        );
    }

    #[test]
    // Native component scope: metadata can raise or lower admission node count.
    fn json_nodes_uses_metadata_above_and_below_the_seed_limit() {
        let seed = policy();
        // The array itself consumes one node in addition to its elements.
        let expected = Value::Array(vec![Value::Null; seed.json_nodes]);
        let bytes = serde_json::to_vec(&expected).unwrap();
        let mut raised = seed.clone();
        raised.json_nodes = seed.json_nodes + 1;

        assert_eq!(parse_fixture(&bytes, &raised).unwrap(), expected);
        assert_eq!(
            parse_fixture(&bytes, &seed).unwrap_err(),
            "native_json_complexity_bound_native_json_nodes"
        );

        let mut lowered = seed.clone();
        lowered.json_nodes = 2;
        assert_eq!(
            parse_fixture(b"[0,0]", &seed).unwrap(),
            serde_json::json!([0, 0])
        );
        assert_eq!(
            parse_fixture(b"[0,0]", &lowered).unwrap_err(),
            "native_json_complexity_bound_native_json_nodes"
        );
    }

    #[test]
    //= spec/impl/v2/implementation.md#the-native-executor-is-bounded
    //= type=test
    //# The native executor MUST terminate its attempt when the task's execution deadline is reached.
    // Native component scope: Deadline stops bounded-reader work inside one long scalar.
    fn deadline_inside_one_large_scalar_is_not_malformed() {
        let bytes = format!("\"{}\"", "a".repeat(6 * 1024 * 1024));
        let result = parse(
            bytes.as_bytes(),
            bytes.len(),
            Instant::now() + Duration::from_millis(1),
            "malformed",
            &policy(),
            "test_bytes",
        );
        assert_eq!(result.err().as_deref(), Some("native_deadline_exceeded"));
    }

    #[test]
    //= spec/impl/v2/implementation.md#the-native-executor-is-bounded
    //= type=test
    //# The native executor MUST terminate its attempt when the task's execution deadline is reached.
    // Native component scope: After value decoding, end-of-input deadline remains Deadline rather than malformed.
    fn deadline_while_finishing_trailing_whitespace_is_not_malformed() {
        let policy = policy();
        let bytes = format!("0{}", " ".repeat(4096));
        let deadline = Instant::now() + Duration::from_millis(20);
        let reader = DeadlineReader {
            bytes: bytes.as_bytes(),
            deadline,
            check_bytes: policy.decode_check_bytes,
            since_check: 0,
            expired: false,
        };
        let mut decoder = serde_json::Deserializer::from_reader(reader);
        let mut work = Work {
            remaining: policy.json_nodes,
            depth_limit: policy.json_depth,
            limit: bytes.len(),
            deadline,
            failure: None,
        };
        let value = Node {
            work: &mut work,
            depth: 0,
        }
        .deserialize(&mut decoder)
        .unwrap();
        assert_eq!(value, Value::from(0));
        // Explicitly finish the value first, then expire the same reader clock:
        // the exercised error is end(), not initial node admission.
        std::thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
        );
        assert_eq!(
            end_checked(&mut decoder, deadline, "malformed")
                .err()
                .as_deref(),
            Some("native_deadline_exceeded")
        );
    }
}

#[cfg(test)]
mod final_format_tests {
    use super::*;
    use std::time::Duration;

    fn policy() -> NativeLimits {
        serde_json::from_str(include_str!("../../limits-native-seed.json")).unwrap()
    }
    fn future() -> Instant { Instant::now() + Duration::from_secs(5) }

    #[test]
    fn final_syntax_and_duplicate_keys_are_typed_without_echoing_input() {
        let p = policy();
        let malformed = b"{\n  \"private_property\": }";
        let issue = parse_final(malformed, malformed.len(), future(), &p, "task_output_bytes")
            .unwrap().unwrap_err();
        assert!(matches!(issue, FormatIssue::InvalidJson { line: 2, column } if column > 0));
        assert_eq!(issue.stable_error(), "native_output_not_json");
        assert!(!issue.feedback().contains("private_property"));
        assert_eq!(parse(malformed, malformed.len(), future(), "old_malformed", &p, "task_output_bytes").unwrap_err(), "old_malformed");

        let duplicate = br#"{"private_key":"private_value","private_key":0}"#;
        let issue = parse_final(duplicate, duplicate.len(), future(), &p, "task_output_bytes")
            .unwrap().unwrap_err();
        assert_eq!(issue, FormatIssue::DuplicateKey {});
        assert_eq!(issue.stable_error(), "native_json_duplicate_key");
        assert!(!issue.feedback().contains("private_key"));
        assert!(!issue.feedback().contains("private_value"));
        assert_eq!(parse(duplicate, duplicate.len(), future(), "old_malformed", &p, "task_output_bytes").unwrap_err(), issue.stable_error());
    }

    #[test]
    fn final_admission_failures_remain_outer_errors_with_original_codes() {
        let bytes = b"[[0]]";
        let check = |p: &NativeLimits, limit, deadline, expected: &str| {
            assert_eq!(parse_final(bytes, limit, deadline, p, "task_output_bytes").unwrap_err(), expected);
            assert_eq!(parse(bytes, limit, deadline, "old_malformed", p, "task_output_bytes").unwrap_err(), expected);
        };
        let p = policy();
        check(&p, 2, future(), "native_json_byte_bound_task_output_bytes");
        let mut nodes = p.clone(); nodes.json_nodes = 1;
        check(&nodes, bytes.len(), future(), "native_json_complexity_bound_native_json_nodes");
        let mut depth = p.clone(); depth.json_depth = 1;
        check(&depth, bytes.len(), future(), "native_json_complexity_bound_native_json_depth");
        check(&p, bytes.len(), Instant::now(), "native_deadline_exceeded");
        let mut configuration = p; configuration.decode_check_bytes = 0;
        check(&configuration, bytes.len(), future(), "native_json_configuration_invalid_native_decode_check_bytes");
    }

    #[test]
    fn final_value_and_trailing_syntax_use_the_same_bounded_parser() {
        let p = policy();
        let bytes = br#"{"ok":[true,null]}"#;
        assert_eq!(parse_final(bytes, bytes.len(), future(), &p, "task_output_bytes").unwrap().unwrap(),
            parse(bytes, bytes.len(), future(), "old_malformed", &p, "task_output_bytes").unwrap());
        let trailing = b"0 1";
        assert!(matches!(parse_final(trailing, trailing.len(), future(), &p, "task_output_bytes").unwrap(),
            Err(FormatIssue::InvalidJson { .. })));
        assert_eq!(parse(trailing, trailing.len(), future(), "old_malformed", &p, "task_output_bytes").unwrap_err(), "old_malformed");
    }
}
