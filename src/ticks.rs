use tokio::time::{Duration, Instant};

const PERIOD_THRESHOLD: Duration = Duration::from_secs(365 * 24 * 3600);

pub(crate) struct Ticks {
    period: Duration,
    max_bias: f64,
    origin: Instant,
    next_at: Option<Instant>,
}

impl Default for Ticks {
    fn default() -> Self {
        Self {
            period: Duration::MAX,
            max_bias: 0.,
            origin: Instant::now(),
            next_at: None,
        }
    }
}

impl Ticks {
    pub(crate) fn set_period(&mut self, period: Option<Duration>) {
        self.period = period.unwrap_or(Duration::MAX);
    }

    pub(crate) fn set_period_bias(&mut self, max_bias: f64) {
        self.max_bias = max_bias.clamp(0., 1.);
    }

    pub(crate) fn next_at(&self) -> Option<Instant> {
        self.next_at
    }

    pub(crate) fn reschedule(&mut self) {
        self.next_at = self.calc_next_at();
    }

    fn calc_next_at(&mut self) -> Option<Instant> {
        // Disabled ticks, do nothing.
        if self.period >= PERIOD_THRESHOLD {
            return None;
        }

        let now = Instant::now();
        let elapsed = now - self.origin;

        let coef = (elapsed.subsec_nanos() & 0xffff) as f64 / 65535.;
        let max_bias = self.period.mul_f64(self.max_bias);
        let bias = max_bias.mul_f64(coef);
        let n = elapsed.as_nanos().checked_div(self.period.as_nanos())?;

        let next_at = self.origin + self.period * (n + 1) as u32 + 2 * bias - max_bias;

        // Special case if after skipping we hit biased zone.
        if next_at <= now {
            next_at.checked_add(self.period)
        } else {
            Some(next_at)
        }
    }
}

#[tokio::test]
async fn it_works() {
    tokio::time::pause();
    let origin = Instant::now();

    // No bias.
    let mut ticks = Ticks::default();
    ticks.set_period(Some(Duration::from_secs(10)));
    ticks.reschedule();

    assert_eq!(ticks.next_at().unwrap() - origin, Duration::from_secs(10));
    tokio::time::advance(Duration::from_secs(3)).await;
    ticks.reschedule();
    assert_eq!(ticks.next_at().unwrap() - origin, Duration::from_secs(10));
    tokio::time::advance(Duration::from_secs(7)).await;
    ticks.reschedule();
    assert_eq!(ticks.next_at().unwrap() - origin, Duration::from_secs(20));

    // Up to 10% bias.
    ticks.set_period_bias(0.1);
    ticks.reschedule();
    assert_eq!(ticks.next_at().unwrap() - origin, Duration::from_secs(19));
    tokio::time::advance(Duration::from_secs(12)).await;
    ticks.reschedule();
    assert_eq!(ticks.next_at().unwrap() - origin, Duration::from_secs(29));

    // Try other seeds.
    tokio::time::advance(Duration::from_nanos(32768)).await;
    ticks.reschedule();
    assert_eq!(
        (ticks.next_at().unwrap() - origin).as_secs_f64().round(),
        30.
    );

    tokio::time::advance(Duration::from_nanos(32767)).await;
    ticks.reschedule();
    assert_eq!(ticks.next_at().unwrap() - origin, Duration::from_secs(31));
}

#[tokio::test]
async fn it_skips_extra_ticks() {
    tokio::time::pause();
    let origin = Instant::now();

    let mut ticks = Ticks::default();
    ticks.set_period(Some(Duration::from_secs(10)));
    ticks.set_period_bias(0.1);
    ticks.reschedule();

    // Trivial case, just skip several ticks.
    assert_eq!(ticks.next_at().unwrap() - origin, Duration::from_secs(9));
    tokio::time::advance(Duration::from_secs(30)).await;
    ticks.reschedule();
    assert_eq!(ticks.next_at().unwrap() - origin, Duration::from_secs(39));

    // Hit biased zone.
    tokio::time::advance(Duration::from_secs(19)).await;
    ticks.reschedule();
    assert_eq!(ticks.next_at().unwrap() - origin, Duration::from_secs(59));
}

#[tokio::test]
async fn it_is_disabled() {
    let mut ticks = Ticks::default();
    assert!(ticks.next_at().is_none());
    ticks.reschedule();
    assert!(ticks.next_at().is_none());

    // Not disabled.
    ticks.set_period(Some(Duration::from_secs(10)));
    ticks.reschedule();
    assert!(ticks.next_at().is_some());

    // Explicitly.
    ticks.set_period(None);
    ticks.reschedule();
    assert!(ticks.next_at().is_none());

    // Zero duration.
    ticks.set_period(Some(Duration::from_secs(0)));
    ticks.reschedule();
    assert!(ticks.next_at().is_none());

    // Too big duration.
    ticks.set_period(Some(PERIOD_THRESHOLD));
    ticks.reschedule();
    assert!(ticks.next_at().is_none());
}
