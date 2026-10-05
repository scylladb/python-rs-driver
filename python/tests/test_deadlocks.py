import subprocess
import sys

import pytest
from helpers.exit_scenarios import CONTACT_POINT

CHILD_TIMEOUT = 30.0


def _run_child(script: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run `script` in a fresh interpreter; a child that never exits is a deadlock."""
    # Unbuffered, so a hung child's output is not lost in its buffers.
    command = [sys.executable, "-u", "-c", script, *args]
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as child:
        try:
            stdout, stderr = child.communicate(timeout=CHILD_TIMEOUT)
        except subprocess.TimeoutExpired:
            # `subprocess.run` drops a timed-out child's output on POSIX; collect it here.
            child.kill()
            stdout, stderr = child.communicate()
            pytest.fail(
                f"the child never exited within {CHILD_TIMEOUT}s: it deadlocked\nstdout: {stdout!r}\nstderr: {stderr!r}"
            )
    return subprocess.CompletedProcess(command, child.returncode, stdout, stderr)


_AFTER_SUBINTERPRETER = """
try:
    import _interpreters as interpreters
except ImportError:
    import _xxsubinterpreters as interpreters
import socket
from scylla.session import SessionBuilder

interpreters.destroy(interpreters.create())

listener = socket.socket()
listener.bind(("127.0.0.1", 0))
listener.listen()

future = SessionBuilder().contact_points([listener.getsockname()]).connection_timeout(3600).connect()
try:
    future.result(timeout=0)
except TimeoutError:
    print("timed out")
"""


def test_attachment_check_survives_a_subinterpreter() -> None:
    """Waiting on a pending future after a subinterpreter was created.

    Creating a subinterpreter makes `PyGILState_Check` return 1 on every thread, so an
    attachment check built on it takes detached threads for attached ones: this panicked
    with "still attached inside py.detach", or in release builds let a detached tokio
    worker lock as if attached. It runs in a child, as the subinterpreter's effect
    is process-wide and permanent.
    """
    completed = _run_child(_AFTER_SUBINTERPRETER)

    assert "panicked" not in completed.stderr, completed.stderr
    assert completed.returncode == 0, f"the child exited with {completed.returncode}\n{completed.stderr}"
    assert "timed out" in completed.stdout, completed.stderr


_CALLBACK_WHILE_PARKING = """
import asyncio
import socket
import threading
from scylla.session import SessionBuilder

parking_entered = threading.Event()
callback_entered = threading.Event()


class HookedLoop(asyncio.SelectorEventLoop):
    armed = False
    callback_seen = None

    def create_future(self):
        future = super().create_future()
        if self.armed and self.callback_seen is None:
            parking_entered.set()
            self.callback_seen = callback_entered.wait(5)
        return future


listener = socket.socket()
listener.bind(("127.0.0.1", 0))
listener.listen()


def fail_the_connection_once_parked():
    conn, _ = listener.accept()
    assert parking_entered.wait(5)
    conn.close()
    listener.close()


threading.Thread(target=fail_the_connection_once_parked, daemon=True).start()


async def main():
    # Well under CHILD_TIMEOUT, so a helper that never closes the connection is not taken for a deadlock.
    future = SessionBuilder().contact_points([listener.getsockname()]).connection_timeout(10).connect()
    future.on_error(lambda _error: callback_entered.set())

    loop.armed = True
    try:
        await future
    except Exception:
        pass
    assert loop.callback_seen


loop = HookedLoop()
loop.run_until_complete(main())
print("done")
"""


def test_callback_completion_while_the_event_loop_parks() -> None:
    """Firing a callback and waking the awaiter while the event loop is parking on it.

    Parking holds the waker's lock across `loop.create_future()`, which here waits with
    the GIL released until the callback has fired. The completion thread then wakes the
    waker: if it waited for that lock still holding the GIL, the loop could never return
    from `create_future` to release it, and the whole process would hang.
    """
    completed = _run_child(_CALLBACK_WHILE_PARKING)

    assert completed.returncode == 0, f"the child exited with {completed.returncode}\n{completed.stderr}"
    assert "done" in completed.stdout, completed.stderr


_CONCURRENT_NEXT = """
import sys
import threading
import time
from scylla.session import SessionBuilder

factory_entered = threading.Event()
second_started = threading.Event()
errors = []
threading.excepthook = lambda args: errors.append(args.exc_value)


def blocking_builder(values):
    factory_entered.set()
    assert second_started.wait(5)
    # Give the second thread time to reach the iterator's lock.
    time.sleep(0.2)
    return values


session = SessionBuilder().contact_points([(sys.argv[1], int(sys.argv[2]))]).connect().result(timeout=10)
result = session.execute("SELECT release_version FROM system.local", factory=blocking_builder).result(timeout=10)
iterator = iter(result.first_page)
rows = []


def first():
    rows.append(next(iterator))


def second():
    second_started.set()
    try:
        next(iterator)
    except StopIteration:
        pass


t1 = threading.Thread(target=first)
t1.start()
assert factory_entered.wait(5)

t2 = threading.Thread(target=second)
t2.start()

t1.join(5)
t2.join(5)
if errors:
    raise errors[0]
assert not t1.is_alive()
assert not t2.is_alive()
assert len(rows) == 1
print("done")
"""


# Best-effort: Python cannot tell when a thread is blocked on a Rust mutex, so the
# contention is near-certain thanks to the sleep, not guaranteed. Good enough for us.
@pytest.mark.requires_db
def test_concurrent_next_while_row_factory_waits() -> None:
    """Two threads calling `next` on one row iterator while the row factory waits.

    Thread A holds the iterator's lock while its factory waits with the GIL released.
    Thread B enters `next`, takes the GIL and waits for the lock. If B kept the GIL while
    waiting, A could never return from the factory to release the lock, and the whole
    process would hang. It runs in a child, so that hang is caught by a timeout.
    """
    host, port = CONTACT_POINT
    completed = _run_child(_CONCURRENT_NEXT, host, str(port))

    assert completed.returncode == 0, f"the child exited with {completed.returncode}\n{completed.stderr}"
    assert "done" in completed.stdout, completed.stderr


_BATCHER_CONTENTION = """
import asyncio
import queue
import socket
import sys
import threading
from scylla.errors import FutureCancelledError
from scylla.session import SessionBuilder

# Tuned by hand: more workers or a shorter switch interval made the old deadlock rarer.
WORKERS = 64
QUERIES = 100000
# Parked futures kept back, so a cancel is at hand whenever drain drops the GIL.
RESERVE = 16

session = SessionBuilder().contact_points([(sys.argv[1], int(sys.argv[2]))]).connect().result(timeout=10)
# Accepts into its backlog and never speaks CQL, so a connect to it stays parked until cancelled.
listener = socket.socket()
listener.bind(("127.0.0.1", 0))
listener.listen(1)
to_cancel = queue.SimpleQueue()
finished = threading.Event()
completed = 0
cancelled = 0
errors = []


def cancel_from_another_thread():
    # Spins instead of blocking, so it is always waiting for the GIL when drain drops it.
    try:
        while not finished.is_set():
            if to_cancel.qsize() <= RESERVE and completed + cancelled < QUERIES:
                continue
            try:
                to_cancel.get_nowait().cancel()
            except queue.Empty:
                pass
    except BaseException as error:
        errors.append(error)


async def worker(cancellable):
    global completed, cancelled
    while completed + cancelled < QUERIES:
        if cancellable:
            future = SessionBuilder().contact_points([listener.getsockname()]).connection_timeout(3600).connect()
            to_cancel.put(future)
        else:
            future = session.execute("SELECT release_version FROM system.local")
        try:
            await future
            completed += 1
        except FutureCancelledError:
            cancelled += 1


async def main():
    await asyncio.gather(*(worker(cancellable=i % 2 == 0) for i in range(WORKERS)))


canceller = threading.Thread(target=cancel_from_another_thread)
canceller.start()
try:
    asyncio.run(main())
finally:
    finished.set()
    canceller.join(5)
if errors:
    raise errors[0]
assert not canceller.is_alive()
assert completed > 0 and cancelled > 0, (completed, cancelled)
print("done")
"""


# Best-effort: every critical section on `Batcher.state` is pure Rust, so no test hook can
# hold one open; the overlap comes from volume instead. Restoring `lock_py_attached` in
# drain hung most runs within a few seconds.
@pytest.mark.requires_db
def test_batcher_push_overlapping_drain() -> None:
    """Attached and detached pushes onto one event loop's batcher while it drains.

    Tokio workers push completed queries without the GIL, and a second thread pushes
    cancelled connects while holding it. If drain released the GIL to wait for `state`
    held by a detached push, it would then hold `state` while waiting for the GIL, and
    the cancelling thread would hold the GIL while waiting for `state`: the whole process
    would hang. It runs in a child, so that hang is caught by a timeout.
    """
    host, port = CONTACT_POINT
    completed = _run_child(_BATCHER_CONTENTION, host, str(port))

    assert completed.returncode == 0, f"the child exited with {completed.returncode}\n{completed.stderr}"
    assert "done" in completed.stdout, completed.stderr
