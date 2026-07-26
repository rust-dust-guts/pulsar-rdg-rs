//! executor abstraction
//!
//! this crate is compatible with Tokio and async-std, by assembling  them
//! under the [Executor] trait
use std::{ops::Deref, pin::Pin, sync::Arc, task::Poll};

use futures::{Future, Stream};

/// indicates which executor is used
pub enum ExecutorKind {
    /// Tokio executor
    Tokio,
    /// async-std executor
    AsyncStd,
}

/// Wrapper trait abstracting the Tokio and async-std executors
pub trait Executor: Clone + Send + Sync + 'static {
    /// spawns a new task
    #[allow(clippy::result_unit_err)]
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()>;

    /// spawns a new blocking task
    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static;

    /// returns a Stream that will produce at regular intervals
    fn interval(&self, duration: std::time::Duration) -> Interval;

    /// waits for a configurable time
    fn delay(&self, duration: std::time::Duration) -> Delay;

    /// returns which executor is currently used
    // test at runtime and manually choose the implementation
    // because we cannot (yet) have async trait methods,
    // so we cannot move the TCP connection here
    fn kind(&self) -> ExecutorKind;
}

/// Wrapper for the Tokio executor
#[cfg(any(
    feature = "tokio-runtime",
    feature = "tokio-rustls-runtime-aws-lc-rs",
    feature = "tokio-rustls-runtime-ring"
))]
#[derive(Clone, Debug)]
pub struct TokioExecutor;

#[cfg(any(
    feature = "tokio-runtime",
    feature = "tokio-rustls-runtime-aws-lc-rs",
    feature = "tokio-rustls-runtime-ring"
))]
impl Executor for TokioExecutor {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        tokio::task::spawn(f);
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        JoinHandle::Tokio(tokio::task::spawn_blocking(f))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn interval(&self, duration: std::time::Duration) -> Interval {
        Interval::Tokio(tokio::time::interval(duration))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn delay(&self, duration: std::time::Duration) -> Delay {
        Delay::Tokio(tokio::time::sleep(duration))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Tokio
    }
}

/// Wrapper for the async-std executor
#[cfg(any(
    feature = "async-std-runtime",
    feature = "async-std-rustls-runtime-aws-lc-rs",
    feature = "async-std-rustls-runtime-ring"
))]
#[derive(Clone, Debug)]
pub struct AsyncStdExecutor;

#[cfg(any(
    feature = "async-std-runtime",
    feature = "async-std-rustls-runtime-aws-lc-rs",
    feature = "async-std-rustls-runtime-ring"
))]
impl Executor for AsyncStdExecutor {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        async_std::task::spawn(f);
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        JoinHandle::AsyncStd(async_std::task::spawn_blocking(f))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn interval(&self, duration: std::time::Duration) -> Interval {
        Interval::AsyncStd(async_std::stream::interval(duration))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn delay(&self, duration: std::time::Duration) -> Delay {
        use async_std::prelude::FutureExt;
        Delay::AsyncStd(Box::pin(async_std::future::ready(()).delay(duration)))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::AsyncStd
    }
}

impl<Exe: Executor> Executor for Arc<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn spawn(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) -> Result<(), ()> {
        self.deref().spawn(f)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn spawn_blocking<F, Res>(&self, f: F) -> JoinHandle<Res>
    where
        F: FnOnce() -> Res + Send + 'static,
        Res: Send + 'static,
    {
        self.deref().spawn_blocking(f)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn interval(&self, duration: std::time::Duration) -> Interval {
        self.deref().interval(duration)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn delay(&self, duration: std::time::Duration) -> Delay {
        self.deref().delay(duration)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn kind(&self) -> ExecutorKind {
        self.deref().kind()
    }
}

/// future returned by [Executor::spawn_blocking] to await on the task's result
pub enum JoinHandle<T> {
    /// wrapper for tokio's `JoinHandle`
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    Tokio(tokio::task::JoinHandle<T>),
    /// wrapper for async-std's `JoinHandle`
    #[cfg(any(
        feature = "async-std-runtime",
        feature = "async-std-rustls-runtime-aws-lc-rs",
        feature = "async-std-rustls-runtime-ring"
    ))]
    AsyncStd(async_std::task::JoinHandle<T>),
    // here to avoid a compilation error since T is not used
    #[cfg(all(
        not(feature = "tokio-runtime"),
        not(feature = "tokio-rustls-runtime-aws-lc-rs"),
        not(feature = "tokio-rustls-runtime-ring"),
        not(feature = "async-std-runtime"),
        not(feature = "async-std-rustls-runtime-aws-lc-rs"),
        not(feature = "async-std-rustls-runtime-ring")
    ))]
    PlaceHolder(T),
}

impl<T> Future for JoinHandle<T> {
    type Output = Option<T>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            #[cfg(any(
                feature = "tokio-runtime",
                feature = "tokio-rustls-runtime-aws-lc-rs",
                feature = "tokio-rustls-runtime-ring"
            ))]
            JoinHandle::Tokio(j) => match Pin::new(j).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(v) => Poll::Ready(v.ok()),
            },
            #[cfg(any(
                feature = "async-std-runtime",
                feature = "async-std-rustls-runtime-aws-lc-rs",
                feature = "async-std-rustls-runtime-ring"
            ))]
            JoinHandle::AsyncStd(j) => match Pin::new(j).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(v) => Poll::Ready(Some(v)),
            },
            #[cfg(all(
                not(feature = "tokio-runtime"),
                not(feature = "tokio-rustls-runtime-aws-lc-rs"),
                not(feature = "tokio-rustls-runtime-ring"),
                not(feature = "async-std-runtime"),
                not(feature = "async-std-rustls-runtime-aws-lc-rs"),
                not(feature = "async-std-rustls-runtime-ring")
            ))]
            JoinHandle::PlaceHolder(t) => {
                unimplemented!("please activate one of the following cargo features: tokio-runtime, async-std-runtime")
            }
        }
    }
}

/// a `Stream` producing a `()` at rgular time intervals
pub enum Interval {
    /// wrapper for tokio's interval
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    Tokio(tokio::time::Interval),
    /// wrapper for async-std's interval
    #[cfg(any(
        feature = "async-std-runtime",
        feature = "async-std-rustls-runtime-aws-lc-rs",
        feature = "async-std-rustls-runtime-ring"
    ))]
    AsyncStd(async_std::stream::Interval),
    #[cfg(all(
        not(feature = "tokio-runtime"),
        not(feature = "tokio-rustls-runtime-aws-lc-rs"),
        not(feature = "tokio-rustls-runtime-ring"),
        not(feature = "async-std-runtime"),
        not(feature = "async-std-rustls-runtime-aws-lc-rs"),
        not(feature = "async-std-rustls-runtime-ring")
    ))]
    PlaceHolder,
}

impl Stream for Interval {
    type Item = ();

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Option<Self::Item>> {
        unsafe {
            match Pin::get_unchecked_mut(self) {
                #[cfg(any(
                    feature = "tokio-runtime",
                    feature = "tokio-rustls-runtime-aws-lc-rs",
                    feature = "tokio-rustls-runtime-ring"
                ))]
                Interval::Tokio(j) => match Pin::new_unchecked(j).poll_tick(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) => Poll::Ready(Some(())),
                },
                #[cfg(any(
                    feature = "async-std-runtime",
                    feature = "async-std-rustls-runtime-aws-lc-rs",
                    feature = "async-std-rustls-runtime-ring"
                ))]
                Interval::AsyncStd(j) => match Pin::new_unchecked(j).poll_next(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(v) => Poll::Ready(v),
                },
                #[cfg(all(
                    not(feature = "tokio-runtime"),
                    not(feature = "tokio-rustls-runtime-aws-lc-rs"),
                    not(feature = "tokio-rustls-runtime-ring"),
                    not(feature = "async-std-runtime"),
                    not(feature = "async-std-rustls-runtime-aws-lc-rs"),
                    not(feature = "async-std-rustls-runtime-ring")
                ))]
                Interval::PlaceHolder => {
                    unimplemented!("please activate one of the following cargo features: tokio-runtime, async-std-runtime")
                }
            }
        }
    }
}

/// a future producing a `()` after some time
pub enum Delay {
    /// wrapper around tokio's `Sleep`
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    Tokio(tokio::time::Sleep),
    /// wrapper around async-std's `Delay`
    #[cfg(any(
        feature = "async-std-runtime",
        feature = "async-std-rustls-runtime-aws-lc-rs",
        feature = "async-std-rustls-runtime-ring"
    ))]
    AsyncStd(Pin<Box<dyn Future<Output = ()> + Send + Sync>>),
}

impl Future for Delay {
    type Output = ();

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context) -> std::task::Poll<Self::Output> {
        unsafe {
            match Pin::get_unchecked_mut(self) {
                #[cfg(any(
                    feature = "tokio-runtime",
                    feature = "tokio-rustls-runtime-aws-lc-rs",
                    feature = "tokio-rustls-runtime-ring"
                ))]
                Delay::Tokio(d) => match Pin::new_unchecked(d).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) => Poll::Ready(()),
                },
                #[cfg(any(
                    feature = "async-std-runtime",
                    feature = "async-std-rustls-runtime-aws-lc-rs",
                    feature = "async-std-rustls-runtime-ring"
                ))]
                Delay::AsyncStd(j) => match Pin::new_unchecked(j).poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(_) => Poll::Ready(()),
                },
            }
        }
    }
}

/// The timer fired before the future completed. Returned by [`timeout`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Elapsed;

impl std::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("operation timed out")
    }
}

impl std::error::Error for Elapsed {}

/// Races `future` against `executor`'s timer.
///
/// Timing goes through [`Executor::delay`] rather than calling into tokio or
/// async-std directly, so it needs no `cfg` and always uses the timer belonging to
/// the executor actually in use.
///
/// That last part is the point. This replaces a pair of `cfg`-selected wrappers
/// around `tokio::time::timeout` and `async_std::future::timeout` — and because the
/// default features enable *both* runtimes, `cfg(feature = "async-std")` was true in
/// the default build, so a `TokioExecutor` consumer was driven by async-std's timer.
pub(crate) async fn timeout<E, F>(
    executor: &E,
    future: F,
    duration: std::time::Duration,
) -> Result<F::Output, Elapsed>
where
    E: Executor,
    F: Future,
{
    use futures::future::{select, Either};

    // Both sides need pinning: a plain `F` is not `Unpin`, and neither is `Delay`
    // (it wraps tokio's `Sleep`).
    let future = std::pin::pin!(future);
    let delay = std::pin::pin!(executor.delay(duration));
    match select(future, delay).await {
        Either::Left((output, _)) => Ok(output),
        Either::Right(((), _)) => Err(Elapsed),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::{channel::mpsc, future::poll_immediate, SinkExt, StreamExt};

    use super::*;

    /// Deliberately huge. These tests advance tokio's *paused* clock, which costs no
    /// wall-clock time, and the discriminator between a virtual and a real timer is
    /// that only the virtual one can be moved. With a one-second tick a CI stall of
    /// over a second would let a real-time timer expire too, and the test would pass
    /// with the bug present.
    const TICK: Duration = Duration::from_secs(3600);

    /// Each executor supplies its own timer.
    ///
    /// Asserted on the `Delay` variant directly, because it is the one check that
    /// cannot pass for the wrong reason and cannot hang.
    #[tokio::test]
    async fn each_executor_supplies_its_own_timer() {
        #[cfg(any(
            feature = "tokio-runtime",
            feature = "tokio-rustls-runtime-aws-lc-rs",
            feature = "tokio-rustls-runtime-ring"
        ))]
        assert!(
            matches!(TokioExecutor.delay(TICK), Delay::Tokio(_)),
            "the tokio executor handed out a non-tokio timer"
        );

        #[cfg(any(
            feature = "async-std-runtime",
            feature = "async-std-rustls-runtime-aws-lc-rs",
            feature = "async-std-rustls-runtime-ring"
        ))]
        assert!(
            matches!(AsyncStdExecutor.delay(TICK), Delay::AsyncStd(_)),
            "the async-std executor handed out a non-async-std timer"
        );
    }

    /// `timeout` must expire off the executor's timer and nothing else.
    ///
    /// The discriminator is that the timeout becomes ready from *advancing tokio's
    /// clock alone*, with no real time passing. A naive version of this test awaited
    /// the timeout after advancing — which any real-time timer eventually satisfies,
    /// so it passed even when the timer came from async-std. Polling exactly once is
    /// what distinguishes them.
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    #[tokio::test(start_paused = true)]
    async fn timeout_expires_from_the_executors_clock_alone() {
        let mut timeout = Box::pin(timeout(
            &TokioExecutor,
            futures::future::pending::<()>(),
            TICK,
        ));

        assert!(
            poll_immediate(&mut timeout).await.is_none(),
            "the timeout was ready before its duration elapsed"
        );

        tokio::time::advance(TICK * 2).await;
        assert_eq!(
            poll_immediate(&mut timeout).await,
            Some(Err(Elapsed)),
            "advancing tokio's clock did not make the timeout ready, so it is \
             waiting on some other timer"
        );
    }

    /// A future that is already ready wins, even against a zero-length timeout.
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    #[tokio::test]
    async fn a_ready_future_beats_an_expired_timer() {
        assert_eq!(
            timeout(&TokioExecutor, async { 7 }, Duration::ZERO).await,
            Ok(7),
            "a ready future must not lose to a zero-length timeout"
        );
    }

    /// Timing out must not consume anything from the stream.
    ///
    /// The consumer loop calls this on `event_rx.next()` once a second forever, so a
    /// timeout that swallowed a queued item would silently drop messages.
    #[cfg(any(
        feature = "tokio-runtime",
        feature = "tokio-rustls-runtime-aws-lc-rs",
        feature = "tokio-rustls-runtime-ring"
    ))]
    #[tokio::test(start_paused = true)]
    async fn timing_out_a_stream_read_loses_no_items() {
        let (mut tx, mut rx) = mpsc::unbounded::<u8>();

        let mut attempt = Box::pin(timeout(&TokioExecutor, rx.next(), TICK));
        assert!(poll_immediate(&mut attempt).await.is_none());
        tokio::time::advance(TICK * 2).await;
        assert_eq!(poll_immediate(&mut attempt).await, Some(Err(Elapsed)));
        drop(attempt);

        // The item arrives only after the timeout, and must still be delivered.
        tx.send(42).await.unwrap();
        assert_eq!(
            timeout(&TokioExecutor, rx.next(), TICK).await,
            Ok(Some(42)),
            "an item sent after a timeout was lost"
        );
    }

    /// Runs `fut` with an independent watchdog, so a helper that never wakes fails
    /// in a second instead of hanging until the CI job times out.
    ///
    /// The watchdog is async-std's own `timeout`, not the helper under test, so a
    /// broken helper cannot mask its own failure.
    #[cfg(any(
        feature = "async-std-runtime",
        feature = "async-std-rustls-runtime-aws-lc-rs",
        feature = "async-std-rustls-runtime-ring"
    ))]
    async fn within_a_second<F: Future>(what: &str, fut: F) -> F::Output {
        match async_std::future::timeout(Duration::from_secs(1), fut).await {
            Ok(output) => output,
            Err(_) => panic!("{what}: the helper never completed — its timer never woke"),
        }
    }

    /// The async-std path must expire on its own timer, with no tokio runtime.
    ///
    /// Runs under `#[async_std::test]`, so nothing has entered a tokio runtime. That
    /// matters: a hard-coded `tokio::time` call panics with "there is no reactor
    /// running" rather than hanging, so this fails loudly. An earlier round replaced
    /// this test with a `Delay`-variant assertion out of a misplaced worry about
    /// hangs — but the variant check only proves what `delay()` returns, not that
    /// `timeout` expires off it.
    #[cfg(any(
        feature = "async-std-runtime",
        feature = "async-std-rustls-runtime-aws-lc-rs",
        feature = "async-std-rustls-runtime-ring"
    ))]
    #[async_std::test]
    async fn async_std_timeout_expires_without_a_tokio_runtime() {
        // Real time here, unlike the paused-clock tests, so keep it short.
        let outcome = within_a_second(
            "async_std_timeout_expires_without_a_tokio_runtime",
            timeout(
                &AsyncStdExecutor,
                futures::future::pending::<()>(),
                Duration::from_millis(50),
            ),
        )
        .await;
        assert_eq!(
            outcome,
            Err(Elapsed),
            "the async-std executor's timeout did not expire on its own timer"
        );
    }

    /// And a future that completes still wins under async-std.
    #[cfg(any(
        feature = "async-std-runtime",
        feature = "async-std-rustls-runtime-aws-lc-rs",
        feature = "async-std-rustls-runtime-ring"
    ))]
    #[async_std::test]
    async fn async_std_timeout_returns_the_future_output() {
        // TICK is an hour, so without the watchdog a helper that failed to notice
        // the future completing would stall the job rather than fail.
        let outcome = within_a_second(
            "async_std_timeout_returns_the_future_output",
            timeout(&AsyncStdExecutor, async { 7 }, TICK),
        )
        .await;
        assert_eq!(outcome, Ok(7), "a ready future lost to an hour-long timer");
    }
}
