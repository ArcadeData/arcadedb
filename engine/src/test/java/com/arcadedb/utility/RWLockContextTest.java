/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.utility;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RWLockContextTest {

  private RWLockContext lockContext;

  @BeforeEach
  void setUp() {
    lockContext = new RWLockContext();
  }

  @Test
  void executeInReadLockReturnsResult() {
    final String result = lockContext.executeInReadLock(() -> "result");
    assertThat(result).isEqualTo("result");
  }

  @Test
  void executeInWriteLockReturnsResult() {
    final Integer result = lockContext.executeInWriteLock(() -> 42);
    assertThat(result).isEqualTo(42);
  }

  @Test
  void executeInReadLockAllowsMultipleReaders() throws Exception {
    final AtomicInteger concurrentReaders = new AtomicInteger(0);
    final AtomicInteger maxConcurrentReaders = new AtomicInteger(0);
    final CountDownLatch startLatch = new CountDownLatch(3);
    final CountDownLatch doneLatch = new CountDownLatch(3);

    for (int i = 0; i < 3; i++) {
      new Thread(() -> {
        lockContext.executeInReadLock(() -> {
          startLatch.countDown();
          final int current = concurrentReaders.incrementAndGet();
          maxConcurrentReaders.updateAndGet(max -> Math.max(max, current));
          try {
            Thread.sleep(100);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          concurrentReaders.decrementAndGet();
          return null;
        });
        doneLatch.countDown();
      }).start();
    }

    doneLatch.await(5, TimeUnit.SECONDS);
    assertThat(maxConcurrentReaders.get()).isGreaterThan(1);
  }

  @Test
  void executeInWriteLockExcludesReaders() throws Exception {
    final AtomicBoolean writerActive = new AtomicBoolean(false);
    final AtomicBoolean readerSawWriter = new AtomicBoolean(false);
    final CountDownLatch writerStarted = new CountDownLatch(1);
    final CountDownLatch readerDone = new CountDownLatch(1);

    // Start writer
    new Thread(() ->
      lockContext.executeInWriteLock(() -> {
        writerActive.set(true);
        writerStarted.countDown();
        try {
          Thread.sleep(200);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        writerActive.set(false);
        return null;
      })).start();

    // Wait for writer to start
    writerStarted.await(1, TimeUnit.SECONDS);

    // Try to read - should wait until writer is done
    new Thread(() -> {
      lockContext.executeInReadLock(() -> {
        readerSawWriter.set(writerActive.get());
        return null;
      });
      readerDone.countDown();
    }).start();

    readerDone.await(5, TimeUnit.SECONDS);

    // Reader should not have seen writer as active (writer was done before reader got lock)
    assertThat(readerSawWriter.get()).isFalse();
  }

  // #8838: the lock is striped by thread, so a writer must exclude readers living on every stripe
  @Test
  void writerExcludesReadersOnEveryStripe() throws Exception {
    final int readers = 64;
    final AtomicInteger activeReaders = new AtomicInteger();
    final AtomicInteger readersSeenInWrite = new AtomicInteger();
    final AtomicInteger writes = new AtomicInteger();
    final AtomicBoolean stop = new AtomicBoolean();
    final CountDownLatch done = new CountDownLatch(readers);

    for (int i = 0; i < readers; i++)
      new Thread(() -> {
        while (!stop.get())
          lockContext.executeInReadLock(() -> {
            activeReaders.incrementAndGet();
            activeReaders.decrementAndGet();
            return null;
          });
        done.countDown();
      }).start();

    for (int w = 0; w < 200; w++)
      lockContext.executeInWriteLock(() -> {
        if (activeReaders.get() != 0)
          readersSeenInWrite.incrementAndGet();
        writes.incrementAndGet();
        return null;
      });

    stop.set(true);
    assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(writes.get()).isEqualTo(200);
    assertThat(readersSeenInWrite.get()).isZero();
  }

  @Test
  void concurrentWritersDoNotDeadlockAndAreExclusive() throws Exception {
    final int writers = 8;
    final int[] counter = new int[1];
    final CountDownLatch done = new CountDownLatch(writers);
    for (int i = 0; i < writers; i++)
      new Thread(() -> {
        for (int k = 0; k < 500; k++)
          lockContext.executeInWriteLock(() -> counter[0]++);
        done.countDown();
      }).start();
    assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
    assertThat(counter[0]).isEqualTo(writers * 500);
  }

  @Test
  void writeLockIsReentrantAndAllowsDowngradeToRead() {
    final String result = lockContext.executeInWriteLock(() ->
        lockContext.executeInWriteLock(() -> lockContext.executeInReadLock(() -> "ok")));
    assertThat(result).isEqualTo("ok");
  }

  @Test
  void blockedWriterDoesNotBlockSameThreadReentrantRead() throws Exception {
    final CountDownLatch readerInside = new CountDownLatch(1);
    final CountDownLatch writerQueued = new CountDownLatch(1);
    final AtomicBoolean reentered = new AtomicBoolean();
    final CountDownLatch done = new CountDownLatch(1);

    new Thread(() -> {
      lockContext.executeInReadLock(() -> {
        readerInside.countDown();
        try {
          writerQueued.await(5, TimeUnit.SECONDS);
          Thread.sleep(200);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        // a writer is queued behind this reader, but the reader's own reentrant read must still succeed
        lockContext.executeInReadLock(() -> {
          reentered.set(true);
          return null;
        });
        return null;
      });
      done.countDown();
    }).start();

    readerInside.await(5, TimeUnit.SECONDS);
    final Thread writer = new Thread(() -> lockContext.executeInWriteLock(() -> null));
    writer.start();
    writerQueued.countDown();
    assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
    writer.join(10_000);
    assertThat(reentered.get()).isTrue();
  }

  @Test
  void executeInReadLockWithExceptionPropagates() {
    assertThatThrownBy(() ->
        lockContext.executeInReadLock(() -> {
          throw new RuntimeException("Read error");
        })
    ).isInstanceOf(RuntimeException.class)
        .hasMessage("Read error");
  }

  @Test
  void executeInWriteLockWithExceptionPropagates() {
    assertThatThrownBy(() ->
        lockContext.executeInWriteLock(() -> {
          throw new RuntimeException("Write error");
        })
    ).isInstanceOf(RuntimeException.class)
        .hasMessage("Write error");
  }

  @Test
  void nestedReadLocks() {
    final String result = lockContext.executeInReadLock(() ->
        lockContext.executeInReadLock(() -> "nested")
    );
    assertThat(result).isEqualTo("nested");
  }

  @Test
  void lockContextExecuteInLock() {
    final LockContext simpleLock = new LockContext();

    final AtomicInteger counter = new AtomicInteger(0);

    simpleLock.executeInLock(() -> {
      counter.incrementAndGet();
      return null;
    });

    assertThat(counter.get()).isEqualTo(1);
  }

  @Test
  void lockContextWithException() {
    final LockContext simpleLock = new LockContext();

    assertThatThrownBy(() ->
        simpleLock.executeInLock(() -> {
          throw new RuntimeException("Lock error");
        })
    ).isInstanceOf(RuntimeException.class)
        .hasMessage("Lock error");
  }

  @Test
  void lockContextReturnsValue() {
    final LockContext simpleLock = new LockContext();

    final Object result = simpleLock.executeInLock(() -> "returnValue");

    assertThat(result).isEqualTo("returnValue");
  }
}
