use crate::id::LogicalTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FaultKind {
    PublishConflict,
    DuplicateEffect,
    DelayedEffect,
    MissingObject,
    WriteIntentExpiry,
    OrphanSegment,
    MissedAsyncFree,
    CrashReplayBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultInjector {
    seed: u64,
}

impl FaultInjector {
    pub const fn new(seed: u64) -> Self {
        Self { seed }
    }

    pub fn should_inject(&self, step: u64, kind: FaultKind) -> bool {
        let kind = kind as u64;
        let mut value = self.seed ^ step.rotate_left(17) ^ kind.wrapping_mul(0x9e37_79b9);
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        value.is_multiple_of(11)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectGraphSummary {
    pub live_devices: usize,
    pub deleted_devices: usize,
    pub native_files: usize,
    pub metadata_nodes: usize,
    pub gc_roots: usize,
    pub referenced_segments: usize,
    pub released_segments: usize,
    pub freed_segments: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureArtifact {
    pub seed: u64,
    pub trace: Vec<String>,
    pub minimized_trace: Vec<String>,
    pub object_graph: ObjectGraphSummary,
}

impl FailureArtifact {
    pub fn new(seed: u64, trace: &[String], object_graph: ObjectGraphSummary) -> Self {
        Self {
            seed,
            trace: trace.to_vec(),
            minimized_trace: minimized_trace_suffix(trace, 32),
            object_graph,
        }
    }
}

fn minimized_trace_suffix(trace: &[String], max_events: usize) -> Vec<String> {
    let start = trace.len().saturating_sub(max_events);
    trace[start..].to_vec()
}

pub fn minimize_trace_by_deletion<T: Clone>(
    trace: &[T],
    mut still_reproduces: impl FnMut(&[T]) -> bool,
) -> Vec<T> {
    let mut candidate = trace.to_vec();
    let mut index = 0;
    while index < candidate.len() {
        let mut attempt = candidate.clone();
        attempt.remove(index);
        if still_reproduces(&attempt) {
            candidate = attempt;
        } else {
            index += 1;
        }
    }
    candidate
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeClock {
    now: LogicalTime,
}

impl FakeClock {
    pub const fn new(now: LogicalTime) -> Self {
        Self { now }
    }

    pub const fn now(&self) -> LogicalTime {
        self.now
    }

    pub fn advance(&mut self, delta: u64) {
        self.now = LogicalTime::from_raw(self.now.raw().saturating_add(delta));
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeededRng {
    state: u64,
}

impl SeededRng {
    pub const fn new(seed: u64) -> Self {
        let state = if seed == 0 {
            0x9e37_79b9_7f4a_7c15
        } else {
            seed
        };
        Self { state }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    pub fn choose_index(&mut self, len: usize) -> Option<usize> {
        if len == 0 {
            return None;
        }

        Some((self.next_u64() as usize) % len)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TraceRecorder {
    events: Vec<String>,
}

impl TraceRecorder {
    pub fn record(&mut self, event: impl Into<String>) {
        self.events.push(event.into());
    }

    pub fn events(&self) -> &[String] {
        &self.events
    }

    pub fn into_events(self) -> Vec<String> {
        self.events
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeterministicHarness {
    pub seed: u64,
    pub rng: SeededRng,
    pub clock: FakeClock,
    pub trace: TraceRecorder,
}

impl DeterministicHarness {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            rng: SeededRng::new(seed),
            clock: FakeClock::new(LogicalTime::from_raw(0)),
            trace: TraceRecorder::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_injector_is_replayable_and_bounded() {
        let first = FaultInjector::new(123);
        let second = FaultInjector::new(123);

        for step in 0..64 {
            for kind in [
                FaultKind::PublishConflict,
                FaultKind::DuplicateEffect,
                FaultKind::DelayedEffect,
                FaultKind::MissingObject,
                FaultKind::WriteIntentExpiry,
                FaultKind::OrphanSegment,
                FaultKind::MissedAsyncFree,
                FaultKind::CrashReplayBoundary,
            ] {
                assert_eq!(
                    first.should_inject(step, kind),
                    second.should_inject(step, kind)
                );
            }
        }
    }

    #[test]
    fn failure_artifact_records_seed_trace_suffix_and_graph_summary() {
        let trace: Vec<_> = (0..40).map(|index| format!("step={index}")).collect();
        let summary = ObjectGraphSummary {
            live_devices: 1,
            deleted_devices: 2,
            native_files: 3,
            metadata_nodes: 4,
            gc_roots: 5,
            referenced_segments: 6,
            released_segments: 7,
            freed_segments: 8,
        };

        let artifact = FailureArtifact::new(9, &trace, summary.clone());

        assert_eq!(artifact.seed, 9);
        assert_eq!(artifact.trace, trace);
        assert_eq!(artifact.minimized_trace.len(), 32);
        assert_eq!(artifact.minimized_trace[0], "step=8");
        assert_eq!(artifact.object_graph, summary);
    }

    #[test]
    fn trace_minimizer_removes_events_while_reproduction_still_holds() {
        let trace = vec![1, 2, 3, 4, 5, 6];
        let minimized = minimize_trace_by_deletion(&trace, |candidate| {
            candidate.contains(&2) && candidate.contains(&5)
        });

        assert_eq!(minimized, vec![2, 5]);
    }

    #[test]
    fn seeded_rng_replays_exactly() {
        let mut first = SeededRng::new(42);
        let mut second = SeededRng::new(42);

        let first_values: Vec<_> = (0..8).map(|_| first.next_u64()).collect();
        let second_values: Vec<_> = (0..8).map(|_| second.next_u64()).collect();

        assert_eq!(first_values, second_values);
    }

    #[test]
    fn deterministic_harness_records_replayable_trace() {
        let mut harness = DeterministicHarness::new(7);
        harness.trace.record(format!("seed={}", harness.seed));
        harness
            .trace
            .record(format!("next={}", harness.rng.next_u64()));
        harness.clock.advance(5);
        harness
            .trace
            .record(format!("time={}", harness.clock.now().raw()));

        assert_eq!(
            harness.trace.events(),
            &[
                "seed=7".to_string(),
                "next=7575888327".to_string(),
                "time=5".to_string(),
            ]
        );
    }

    #[test]
    fn seeded_rng_choice_handles_empty_and_bounded_sets() {
        let mut rng = SeededRng::new(11);

        assert_eq!(rng.choose_index(0), None);

        for len in 1..16 {
            let chosen = rng.choose_index(len).unwrap();
            assert!(chosen < len);
        }
    }

    #[test]
    fn fake_clock_advances_deterministically() {
        let mut clock = FakeClock::new(LogicalTime::from_raw(10));

        clock.advance(5);
        assert_eq!(clock.now(), LogicalTime::from_raw(15));
    }
}
