package com.arcadedb.opengrep;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class ConcurrencyShapes {
  private final Lock          lock  = new ReentrantLock();
  private final Lock          other = new ReentrantLock();
  private final Lock[]        locks = { new ReentrantLock(), new ReentrantLock() };
  private volatile int        counter;
  private volatile long       total;
  private          int        plain;

  void swallowed() {
    try {
      Thread.sleep(10);
      // ruleid: interrupted-exception-swallowed, empty-catch
    } catch (final InterruptedException e) {
      // nothing
    }
  }

  void restored() {
    try {
      Thread.sleep(10);
      // ok: interrupted-exception-swallowed
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  void rethrown() {
    try {
      Thread.sleep(10);
      // ok: interrupted-exception-swallowed
    } catch (final InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  void deferred() {
    boolean interrupted = false;
    try {
      Thread.sleep(10);
      // ok: interrupted-exception-swallowed
    } catch (final InterruptedException e) {
      interrupted = true;
    }
    if (interrupted)
      Thread.currentThread().interrupt();
  }

  void waitOnce(final Object monitor) throws InterruptedException {
    synchronized (monitor) {
      // ruleid: wait-outside-a-loop
      monitor.wait(10);
    }
  }

  void waitInALoop(final Object monitor) throws InterruptedException {
    synchronized (monitor) {
      while (plain == 0)
        // ok: wait-outside-a-loop
        monitor.wait(10);
    }
  }

  void lockThenWork() {
    // ruleid: lock-not-released-in-finally
    lock.lock();
    final Object resource = open();
    try {
      use(resource);
    } finally {
      lock.unlock();
    }
  }

  void lockThenTry() {
    // ok: lock-not-released-in-finally
    lock.lock();
    try {
      use(null);
    } finally {
      lock.unlock();
    }
  }

  void twoLocksThenTry() {
    lock.lock();
    // ok: lock-not-released-in-finally
    other.lock();
    try {
      use(null);
    } finally {
      other.unlock();
      lock.unlock();
    }
  }

  void lockThenTryWithAnUnrelatedFinally() {
    // ruleid: lock-not-released-in-finally
    lock.lock();
    try {
      use(null);
    } finally {
      use(this);
    }
  }

  void lockThenTryReleasingAnotherLock() {
    // ruleid: lock-not-released-in-finally
    lock.lock();
    try {
      use(null);
    } finally {
      other.unlock();
    }
  }

  void lockInsideAnUnrelatedTry() {
    try {
      // ruleid: lock-not-released-in-finally
      lock.lock();
      use(null);
      lock.unlock();
    } finally {
      use(this);
    }
  }

  void lockInsideATryReleasingIt() {
    try {
      // ok: lock-not-released-in-finally
      lock.lock();
      use(null);
    } finally {
      lock.unlock();
    }
  }

  void conditionalLock(final boolean shared) {
    // ok: lock-not-released-in-finally
    if (shared) lock.lock();
    try {
      use(null);
    } finally {
      if (shared)
        lock.unlock();
    }
  }

  void everyLockOfAnArrayReleased() {
    for (int i = 0; i < locks.length; i++)
      // ok: lock-not-released-in-finally
      locks[i].lock();
    try {
      use(null);
    } finally {
      for (int i = 0; i < locks.length; i++)
        locks[i].unlock();
    }
  }

  void everyLockOfAnArrayTakenAnotherReleased() {
    for (int i = 0; i < locks.length; i++)
      // ruleid: lock-not-released-in-finally
      locks[i].lock();
    try {
      use(null);
    } finally {
      other.unlock();
    }
  }

  void unguardedIncrement() {
    // ruleid: volatile-read-modify-write
    counter++;
    // ruleid: volatile-read-modify-write
    total += 2;
  }

  synchronized void guardedIncrement() {
    // ok: volatile-read-modify-write
    counter++;
  }

  void incrementUnderALock() {
    lock.lock();
    try {
      // ok: volatile-read-modify-write
      counter++;
    } finally {
      lock.unlock();
    }
  }

  void incrementAfterTheUnlock() {
    lock.lock();
    lock.unlock();
    // ruleid: volatile-read-modify-write
    counter++;
  }

  void plainIncrement() {
    // ok: volatile-read-modify-write
    plain++;
  }

  void emptyCatch() {
    try {
      use(null);
      // ruleid: empty-catch
    } catch (final RuntimeException e) {
    }
  }

  void catchThrowable() {
    try {
      use(null);
      // ruleid: catch-throwable-not-rethrown
    } catch (final Throwable t) {
      plain = 1;
    }
  }

  void catchThrowableRethrown() {
    try {
      use(null);
      // ok: catch-throwable-not-rethrown
    } catch (final Throwable t) {
      plain = 1;
      throw t;
    }
  }

  private Object open() {
    return new Object();
  }

  private void use(final Object o) {
  }
}
