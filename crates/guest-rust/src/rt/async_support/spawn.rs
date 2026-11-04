// TODO: Switch to interior mutability (e.g. use Mutexes or thread-local
// RefCells) and remove this, since even in single-threaded mode `static mut`
// references can be a hazard due to recursive access.
#![allow(static_mut_refs)]

use crate::rt::async_support::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use std::boxed::Box;
use std::future::Future;
use std::task::{Context, Poll};
use std::vec::Vec;

/// Any newly-deferred work queued by calls to the `spawn` function while
/// polling the current task.
static mut SPAWNED: Vec<BoxFuture> = Vec::new();

#[derive(Default)]
pub struct Tasks<'a> {
    tasks: FuturesUnordered<BoxFuture<'a>>,
}

impl<'a> Tasks<'a> {
    pub fn new(root: BoxFuture<'a>) -> Tasks<'a> {
        Tasks {
            tasks: [root].into_iter().collect(),
        }
    }

    /// Poll the tasks, and every task one of them spawned while being
    /// polled, until one completes or none can progress.
    ///
    /// A task queued by [`spawn`] is moved into the set after the poll that
    /// queued it, and moving it in wakes nothing. Returning `Pending` then
    /// would leave it unpolled until some other event arrived for this
    /// computation, so the set is polled again whenever a poll that came back
    /// `Pending` moved spawned tasks in: they are polled before this returns.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<()>> {
        loop {
            let ret = self.tasks.poll_next_unpin(cx);
            let spawned = unsafe {
                if SPAWNED.is_empty() {
                    false
                } else {
                    self.tasks.extend(SPAWNED.drain(..));
                    true
                }
            };
            match ret {
                Poll::Pending if spawned => continue,
                ret => return ret,
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

/// Spawn the provided `future` to get executed concurrently with the
/// currently-running async computation.
///
/// This API is somewhat similar to `tokio::task::spawn` for example but has a
/// number of limitations to be aware of. If possible it's recommended to avoid
/// this, but it can be convenient if these limitations do not apply to you:
///
/// * Spawned tasks do not work when the version of the `wit-bindgen` crate
///   managing the export bindings is different from the version of this crate.
///   To work correctly the `spawn` function and export executor must be at
///   exactly the same version. Given the major-version-breaking nature of
///   `wit-bindgen` this is not always easy to rely on. This is tracked in
///   [#1305].
///
/// * Spawned tasks do not outlive the scope of the async computation they are
///   spawned within. For example with an async export function spawned tasks
///   will be polled within the context of that component-model async task. For
///   computations executing within a [`block_on`] call, however, the spawned
///   tasks will be executed within that scope. This notably means that for
///   [`block_on`] spawned tasks will prevent the [`block_on`] function from
///   returning, even if a value is available to return.
///
/// * There is no handle returned to the spawned task meaning that it cannot be
///   cancelled or monitored.
///
/// * The task spawned here is executed *concurrently*, not in *parallel*. This
///   means that while one future is being polled no other future can be polled
///   at the same time. This is similar to a single-thread executor in Tokio.
///
/// With these restrictions in mind this can be used to express
/// execution-after-returning in the component model. For example once an
/// exported async function has produced a value this can be used to continue to
/// execute some more code before the component model async task exits.
///
/// [`block_on`]: crate::block_on
/// [#1305]: https://github.com/bytecodealliance/wit-bindgen/issues/1305
pub fn spawn(future: impl Future<Output = ()> + 'static) {
    unsafe { SPAWNED.push(Box::pin(future)) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Wake, Waker};

    /// A waker that records being woken.
    struct Woken(AtomicBool);

    impl Wake for Woken {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    // One test, because `SPAWNED` is one queue for the whole process and
    // tests in one binary run on several threads.
    #[test]
    fn a_task_spawned_while_the_others_wait_is_polled_before_poll_next_returns() {
        for _ in 0..2 {
            // A root that spawns a task and then waits on something only that
            // task can finish, the way a caller awaits the answer of work it
            // spawned.
            let answered = Rc::new(Cell::new(false));
            let waiting: Rc<Cell<Option<Waker>>> = Rc::new(Cell::new(None));
            let (spawned_answered, spawned_waiting) = (answered.clone(), waiting.clone());
            let (root_answered, root_waiting) = (answered.clone(), waiting.clone());
            let root: BoxFuture<'_> = Box::pin(async move {
                spawn(async move {
                    spawned_answered.set(true);
                    if let Some(waker) = spawned_waiting.take() {
                        waker.wake();
                    }
                });
                std::future::poll_fn(|cx| {
                    if root_answered.get() {
                        Poll::Ready(())
                    } else {
                        root_waiting.set(Some(cx.waker().clone()));
                        Poll::Pending
                    }
                })
                .await;
            });
            let mut tasks = Tasks::new(root);
            let mut cx = Context::from_waker(Waker::noop());

            // The spawned task runs within the first poll; before, it sat in
            // the set unpolled while this answered `Pending` and nothing was
            // woken, so an executor would wait for an event that only the
            // spawned task could cause.
            let first = tasks.poll_next(&mut cx);
            assert!(answered.get(), "the spawned task was not polled");
            assert_eq!(first, Poll::Ready(Some(())), "the spawned task completed");

            // The root, which the spawned task's answer unblocks, completes
            // once it is polled again, and then nothing is left.
            loop {
                match tasks.poll_next(&mut cx) {
                    Poll::Ready(Some(())) => continue,
                    Poll::Ready(None) => break,
                    Poll::Pending => panic!("the root waits on nothing"),
                }
            }
            assert!(tasks.is_empty());

            // The case an executor stalls in: another task waits, the root is
            // the only one ready, and it spawns work and waits for it. The
            // set's poll ends with nothing left to poll and wakes nothing, so
            // the spawned task must be polled by this call or by none.
            let companion_waiting = Rc::new(Cell::new(false));
            let called = Rc::new(Cell::new(false));
            let (companion_flag, root_flag) =
                (companion_waiting.clone(), companion_waiting.clone());
            let (spawned_called, root_called) = (called.clone(), called.clone());
            let mut tasks = Tasks::new(Box::pin(async move {
                spawn(std::future::poll_fn(move |_| {
                    companion_flag.set(true);
                    Poll::<()>::Pending
                }));
                while !root_flag.get() {
                    let mut yielded = false;
                    std::future::poll_fn(|cx| {
                        if yielded {
                            Poll::Ready(())
                        } else {
                            yielded = true;
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    })
                    .await;
                }
                spawn(async move { spawned_called.set(true) });
                std::future::poll_fn(|_| {
                    if root_called.get() {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
            }));
            // Polled the way an executor polls: again only once woken.
            let woken = Arc::new(Woken(AtomicBool::new(true)));
            let waker = Waker::from(woken.clone());
            let mut woken_cx = Context::from_waker(&waker);
            while woken.0.swap(false, Ordering::SeqCst) && !called.get() {
                let _ = tasks.poll_next(&mut woken_cx);
            }
            assert!(
                called.get(),
                "the task spawned beside a waiting one was never polled"
            );

            // A task that spawns nothing and waits leaves `poll_next` answering
            // `Pending` without spinning.
            let mut idle = Tasks::new(Box::pin(std::future::pending::<()>()));
            assert_eq!(idle.poll_next(&mut cx), Poll::Pending);
            unsafe { assert!(SPAWNED.is_empty()) };
        }
    }
}
