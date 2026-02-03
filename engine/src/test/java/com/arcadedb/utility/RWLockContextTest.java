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
    new Thread(() -> {
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
      });
    }).start();

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
