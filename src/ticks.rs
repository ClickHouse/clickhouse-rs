use tokio::time::{Duration, Instant};

pub(crate) struct Ticks {
    period: Duration,
    max_bias: Duration,
    origin: Instant,
    next_at: Instant,
}

impl Ticks {
    pub(crate) fn new(period: Duration, max_bias: f64) -> Self {
        assert!(
            (0.0..=1.0).contains(&max_bias),
            "`max_bias` must be between 0.0 and 1.0"
        );

        let now = Instant::now();
        let mut this = Self {
            period,
            max_bias: period.mul_f64(max_bias),
            origin: now,
            next_at: now,
        };
        this.reschedule();
        this
    }

    pub(crate) fn configure(&mut self, period: Option<Duration>, max_bias: Option<f64>) {
        if let Some(max_bias) = max_bias {
            assert!(
                (0.0..=1.0).contains(&max_bias),
                "`max_bias` must be between 0.0 and 1.0"
            );
        }

        let period = period.unwrap_or(self.period);
        let max_bias = max_bias.map_or(self.max_bias, |b| period.mul_f64(b));

        if period != self.period || max_bias != self.max_bias {
            self.period = period;
            self.max_bias = max_bias;
            self.reschedule();
        }
    }

    pub(crate) fn next_at(&self) -> Instant {
        self.next_at
    }

    pub(crate) fn reschedule(&mut self) {
        let now = Instant::now();
        let elapsed = now - self.origin;

        let coef = (elapsed.subsec_nanos() & 0xffff) as f64 / 65535.;
        let bias = self.max_bias.mul_f64(coef);
        let n = elapsed.as_nanos() / self.period.as_nanos();

        // TODO: overflow
        self.next_at = self.origin + (n + 1) as u32 * self.period + 2 * bias - self.max_bias;

        // Special case if after skipping we hit biased zone.
        if self.next_at <= now {
            self.next_at += self.period;
        }
    }
}

#[tokio::test]
async fn it_works() {
    tokio::time::pause();

    let origin = Instant::now();

    // No bias.
    let mut ticks = Ticks::new(Duration::from_secs(10), 0.);

    assert_eq!(ticks.next_at() - origin, Duration::from_secs(10));
    tokio::time::advance(Duration::from_secs(3)).await;
    ticks.reschedule();
    assert_eq!(ticks.next_at() - origin, Duration::from_secs(10));
    tokio::time::advance(Duration::from_secs(7)).await;
    ticks.reschedule();
    assert_eq!(ticks.next_at() - origin, Duration::from_secs(20));

    // Up to 10% bias.
    ticks.configure(None, Some(0.1));
    assert_eq!(ticks.next_at() - origin, Duration::from_secs(19));
    tokio::time::advance(Duration::from_secs(12)).await;
    ticks.reschedule();
    assert_eq!(ticks.next_at() - origin, Duration::from_secs(29));

    // Try other seeds.
    tokio::time::advance(Duration::from_nanos(32768)).await;
    ticks.reschedule();
    assert_eq!((ticks.next_at() - origin).as_secs_f64().round(), 30.);

    tokio::time::advance(Duration::from_nanos(32767)).await;
    ticks.reschedule();
    assert_eq!(ticks.next_at() - origin, Duration::from_secs(31));
}

#[tokio::test]
async fn it_skips_extra_ticks() {
    tokio::time::pause();

    let origin = Instant::now();

    let mut ticks = Ticks::new(Duration::from_secs(10), 0.1);

    // Trivial case, just skip several ticks.
    assert_eq!(ticks.next_at() - origin, Duration::from_secs(9));
    tokio::time::advance(Duration::from_secs(30)).await;
    ticks.reschedule();
    assert_eq!(ticks.next_at() - origin, Duration::from_secs(39));

    // Hit biased zone.
    tokio::time::advance(Duration::from_secs(19)).await;
    ticks.reschedule();
    assert_eq!(ticks.next_at() - origin, Duration::from_secs(59));
}
