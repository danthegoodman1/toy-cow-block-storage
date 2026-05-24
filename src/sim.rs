use crate::id::LogicalTime;

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
