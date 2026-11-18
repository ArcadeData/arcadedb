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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11921 review: how an in-place Ratis restart acquires the publication gate before it replaces the state machine.
 * {@code restartRatis} tells an abandoned restart (shutdown or interrupt) from a failed one by reading the thread's interrupt
 * flag after the throw, so an interrupt while waiting at the gate must leave the flag set, or the restart is counted toward
 * {@code arcadedb.ha.ratisRestartMaxRetries} as a failure.
 */
class Issue11921PublicationGateTest {

  @Test
  void anInterruptWhileWaitingForAPublicationKeepsTheInterruptFlag() throws Exception {
    assertInterruptKeepsTheFlag(60_000);
  }

  @Test
  void anInterruptWhileWaitingWithoutABoundKeepsTheInterruptFlag() throws Exception {
    assertInterruptKeepsTheFlag(0);
  }

  @Test
  void aPublicationOutlastingTheWaitFailsTheReplacement() throws Exception {
    final ReentrantReadWriteLock gate = new ReentrantReadWriteLock();
    final CountDownLatch publishing = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Thread publication = startPublication(gate, publishing, release);
    try {
      assertThat(publishing.await(10, TimeUnit.SECONDS)).isTrue();
      assertThatThrownBy(() -> RaftHAServer.lockPublicationGateForReplacement(gate.writeLock(), 50))
          .isInstanceOf(IOException.class).hasMessageContaining("published outside the Raft log");
      assertThat(gate.isWriteLockedByCurrentThread()).isFalse();
    } finally {
      release.countDown();
      publication.join(10_000);
    }
  }

  @Test
  void theReplacementGetsTheGateOnceThePublicationReturns() throws Exception {
    final ReentrantReadWriteLock gate = new ReentrantReadWriteLock();
    RaftHAServer.lockPublicationGateForReplacement(gate.writeLock(), 1_000);
    try {
      assertThat(gate.isWriteLockedByCurrentThread()).isTrue();
    } finally {
      gate.writeLock().unlock();
    }
  }

  private static void assertInterruptKeepsTheFlag(final long waitMs) throws Exception {
    final ReentrantReadWriteLock gate = new ReentrantReadWriteLock();
    final CountDownLatch publishing = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Thread publication = startPublication(gate, publishing, release);
    final AtomicReference<Throwable> thrown = new AtomicReference<>();
    final AtomicBoolean interruptedAfterTheThrow = new AtomicBoolean();
    try {
      assertThat(publishing.await(10, TimeUnit.SECONDS)).isTrue();
      final Thread restart = new Thread(() -> {
        try {
          RaftHAServer.lockPublicationGateForReplacement(gate.writeLock(), waitMs);
          gate.writeLock().unlock();
        } catch (final Throwable t) {
          thrown.set(t);
          interruptedAfterTheThrow.set(Thread.currentThread().isInterrupted());
        }
      }, "issue11921-restart");
      restart.setDaemon(true);
      restart.start();

      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (!gate.hasQueuedThread(restart) && System.nanoTime() < deadline)
        Thread.sleep(1);
      assertThat(gate.hasQueuedThread(restart)).as("the restart must be waiting at the gate").isTrue();

      restart.interrupt();
      restart.join(10_000);

      assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
      assertThat(interruptedAfterTheThrow.get()).as("the interrupt flag restartRatis reads to tell an abandoned restart").isTrue();
    } finally {
      release.countDown();
      publication.join(10_000);
    }
  }

  private static Thread startPublication(final ReentrantReadWriteLock gate, final CountDownLatch publishing,
      final CountDownLatch release) {
    final Thread publication = new Thread(() -> {
      gate.readLock().lock();
      try {
        publishing.countDown();
        release.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        gate.readLock().unlock();
      }
    }, "issue11921-publication");
    publication.setDaemon(true);
    publication.start();
    return publication;
  }
}
