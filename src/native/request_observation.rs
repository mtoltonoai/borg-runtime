//! Bounded, best-effort local HTTP diagnostics; never execution/accounting state.
//! A dispatch sample precedes polling reqwest's send future. It includes client
//! pool/DNS/connect/TLS waits and does not prove bytes reached the provider.
//! A terminal sample closes only this local future, never provider-side work.
use super::ValidationHost;
use crate::protocol::Attempt;
use serde::Serialize;
use std::{
    io::{Read, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, SyncSender},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, Serialize)]
pub(super) struct Correlation {
    task_id: String,
    attempt_id: String,
    generation: u64,
}
impl Correlation {
    pub(super) fn from_attempt(attempt: &Attempt) -> Option<Self> {
        // Only the controller's generated identifiers, never arbitrary accepted
        // identifier strings, operator configuration, task inputs or credentials.
        let task = attempt.task_id.strip_prefix("task-")?;
        let (database, sequence) = task.split_once('-')?;
        if database.len() != 32
            || !database
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            || sequence.len() != 20
            || !sequence.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let counter = attempt
            .id
            .strip_prefix("attempt-")?
            .strip_prefix(&attempt.task_id)?
            .strip_prefix('-')?;
        if counter.is_empty() || counter.len() > 20 || !counter.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        Some(Self {
            task_id: attempt.task_id.clone(),
            attempt_id: attempt.id.clone(),
            generation: attempt.generation,
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Phase {
    DispatchPending,
    HeadersReceived,
    Terminal,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum End {
    BodyComplete,
    HttpRefused,
    TransportFailed,
    BodyFailed,
    ByteBound,
    Deadline,
    // The sender cannot distinguish outer cancellation, deadline, or direct
    // future abort. Do not claim a provider cancellation or infer its outcome.
    FutureDropped,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct Record {
    pub event: &'static str,
    pub pid: u32,
    pub process_start_ticks: Option<u64>,
    pub request: u64,
    pub event_sequence: u64,
    pub correlation: Option<Correlation>,
    pub phase: Phase,
    pub dispatch_ns: u64,
    pub headers_ns: Option<u64>,
    pub body_eof_ns: Option<u64>,
    pub end_ns: Option<u64>,
    pub http_status: Option<u16>,
    pub end: Option<End>,
    // All monotonic samples use one process-local origin. UTC was sampled
    // between origin and anchor_after_ns, not at an asserted exact instant.
    pub anchor_unix_ns: Option<u64>,
    pub anchor_after_ns: u64,
    pub enqueue_dropped_total: u64,
    pub write_failed_total: u64,
}

/// Content-free samples of the exact clock used by HTTP request intervals.
/// This is separate from HTTP event_sequence: probes cannot create HTTP gaps.
#[derive(Clone, Debug, Serialize)]
pub(super) struct ClockRecord {
    pub event: &'static str,
    pub pid: u32,
    pub process_start_ticks: Option<u64>,
    pub probe_sequence: u64,
    pub elapsed_ns: u64,
    pub enqueue_dropped_total: u64,
    pub write_failed_total: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub(super) enum Observation {
    Http(Record),
    Clock(ClockRecord),
}
impl Observation {
    fn refresh_loss(&mut self, loss: &Loss) {
        let enqueue = loss.enqueue.load(Ordering::Relaxed);
        let write = loss.write.load(Ordering::Relaxed);
        match self {
            Self::Http(record) => {
                record.enqueue_dropped_total = enqueue;
                record.write_failed_total = write;
            }
            Self::Clock(record) => {
                record.enqueue_dropped_total = enqueue;
                record.write_failed_total = write;
            }
        }
    }
}

#[derive(Default)]
struct Loss {
    enqueue: AtomicU64,
    write: AtomicU64,
}
fn lost(counter: &AtomicU64) {
    // A representation overflow must never erase evidence of prior loss.
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(1))
    });
}

#[derive(Clone)]
pub(super) struct Sink {
    tx: SyncSender<Observation>,
    loss: Arc<Loss>,
}
impl Sink {
    fn emit(&self, mut record: Observation) {
        record.refresh_loss(&self.loss);
        // Never block the executor on diagnostics, even if stderr is wedged.
        if self.tx.try_send(record).is_err() {
            lost(&self.loss.enqueue);
        }
    }

    #[cfg(test)]
    pub(super) fn capture(capacity: usize) -> (Arc<Self>, mpsc::Receiver<Observation>) {
        let (tx, rx) = mpsc::sync_channel(capacity);
        (
            Arc::new(Self {
                tx,
                loss: Arc::default(),
            }),
            rx,
        )
    }
}

fn write_record(output: &mut impl Write, mut record: Observation, loss: &Loss) {
    // Refresh at write time so a drained queue can expose tail loss.
    record.refresh_loss(loss);
    let mut line = Vec::with_capacity(1024);
    if serde_json::to_writer(&mut line, &record).is_err() {
        lost(&loss.write);
        return;
    }
    line.push(b'\n');
    if output.write_all(&line).is_err() {
        lost(&loss.write);
    }
}

pub(super) struct Process {
    origin: Instant,
    anchor_unix_ns: Option<u64>,
    anchor_after_ns: u64,
    start_ticks: Option<u64>,
    request: AtomicU64,
    event: AtomicU64,
    probe: AtomicU64,
    sink: Sink,
}
fn nanoseconds(value: std::time::Duration) -> u64 {
    value.as_nanos().min(u64::MAX as u128) as u64
}
fn start_ticks(max_bytes: usize) -> Option<u64> {
    // One bounded read of this process's public kernel identity; no argv/env.
    let mut text = String::new();
    std::fs::File::open("/proc/self/stat")
        .ok()?
        .take(u64::try_from(max_bytes).ok()?.checked_add(1)?)
        .read_to_string(&mut text)
        .ok()?;
    if text.len() > max_bytes {
        return None;
    }
    text.rsplit_once(')')?
        .1
        .split_whitespace()
        .nth(19)?
        .parse()
        .ok()
}
impl Process {
    pub(super) fn new(worker: &crate::limits::WorkerLimits) -> Self {
        let origin = Instant::now();
        let anchor_unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| u64::try_from(d.as_nanos()).ok());
        let anchor_after_ns = nanoseconds(origin.elapsed());
        let (tx, rx) = mpsc::sync_channel::<Observation>(worker.native_observation_queue_records);
        let loss = Arc::new(Loss::default());
        let writer_loss = loss.clone();
        // Exactly one bounded queue and writer per startup-owned runtime. A
        // stalled writer consumes a fixed thread/queue, not a thread per request.
        let _ = std::thread::Builder::new()
            .name("borg-native-http-log".into())
            .spawn(move || {
                let mut stderr = std::io::stderr();
                for record in rx {
                    write_record(&mut stderr, record, &writer_loss);
                }
            });
        Process {
            origin,
            anchor_unix_ns,
            anchor_after_ns,
            start_ticks: start_ticks(worker.native_observation_proc_bytes),
            request: AtomicU64::new(0),
            event: AtomicU64::new(0),
            probe: AtomicU64::new(0),
            sink: Sink { tx, loss },
        }
    }

    pub(super) fn probe_clock(&self) {
        let Ok(previous) = self
            .probe
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
        else {
            lost(&self.sink.loss.enqueue);
            return;
        };
        let Ok(elapsed_ns) = u64::try_from(self.origin.elapsed().as_nanos()) else {
            lost(&self.sink.loss.enqueue);
            return;
        };
        self.sink.emit(Observation::Clock(ClockRecord {
            event: "borg_native_clock_v1",
            pid: std::process::id(),
            process_start_ticks: self.start_ticks,
            probe_sequence: previous + 1,
            elapsed_ns,
            enqueue_dropped_total: 0,
            write_failed_total: 0,
        }));
    }
}

pub(super) struct Interval<'a> {
    process: &'a Process,
    sink: &'a Sink,
    record: Record,
}
impl<'a> Interval<'a> {
    pub(super) fn dispatch(host: &'a ValidationHost) -> Self {
        let process = &host.runtime.request_process;
        #[cfg(test)]
        let sink = host.request_capture.as_deref().unwrap_or(&process.sink);
        #[cfg(not(test))]
        let sink = &process.sink;
        let mut value = Self {
            process,
            sink,
            record: Record {
                event: "borg_native_http_v1",
                pid: std::process::id(),
                process_start_ticks: process.start_ticks,
                request: process.request.fetch_add(1, Ordering::Relaxed) + 1,
                event_sequence: 0,
                correlation: host.request_correlation.clone(),
                phase: Phase::DispatchPending,
                dispatch_ns: nanoseconds(process.origin.elapsed()),
                headers_ns: None,
                body_eof_ns: None,
                end_ns: None,
                http_status: None,
                end: None,
                anchor_unix_ns: process.anchor_unix_ns,
                anchor_after_ns: process.anchor_after_ns,
                enqueue_dropped_total: 0,
                write_failed_total: 0,
            },
        };
        value.emit();
        value
    }
    fn emit(&mut self) {
        self.record.event_sequence = self.process.event.fetch_add(1, Ordering::Relaxed) + 1;
        self.sink.emit(Observation::Http(self.record.clone()));
    }
    pub(super) fn headers(&mut self, status: u16) {
        self.record.headers_ns = Some(nanoseconds(self.process.origin.elapsed()));
        self.record.http_status = Some(status);
        self.record.phase = Phase::HeadersReceived;
        self.emit();
    }
    pub(super) fn body_eof(&mut self) {
        self.record.body_eof_ns = Some(nanoseconds(self.process.origin.elapsed()));
    }
    pub(super) fn finish(&mut self, end: End) {
        if self.record.end.is_some() {
            return;
        }
        self.record.end_ns = Some(nanoseconds(self.process.origin.elapsed()));
        self.record.end = Some(end);
        self.record.phase = Phase::Terminal;
        self.emit();
    }
}
impl Drop for Interval<'_> {
    fn drop(&mut self) {
        self.finish(End::FutureDropped);
    }
}

#[cfg(test)]
#[path = "request_observation_tests.rs"]
mod tests;
