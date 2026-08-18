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
package com.arcadedb.server.network;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the pre-authentication connection cap of issue #6412.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PreAuthConnectionGateTest {

  @Test
  void connectionsPastTheCapAreRefused() {
    final PreAuthConnectionGate gate = new PreAuthConnectionGate("TEST", 2);

    assertThat(gate.accept()).isNotNull();
    assertThat(gate.accept()).isNotNull();

    assertThat(gate.accept()).as("the third connection is over the cap").isNull();
    assertThat(gate.getPending()).isEqualTo(2);
    assertThat(gate.getRefused()).isEqualTo(1);
  }

  @Test
  void aReleasedPermitLetsTheNextConnectionIn() {
    final PreAuthConnectionGate gate = new PreAuthConnectionGate("TEST", 1);

    final PreAuthConnectionGate.Ticket ticket = gate.accept();
    assertThat(ticket).isNotNull();
    assertThat(gate.accept()).isNull();

    ticket.release();

    assertThat(gate.getPending()).isZero();
    assertThat(gate.accept()).isNotNull();
  }

  @Test
  void releasingTwiceGivesBackOnlyOnePermit() {
    // A connection releases when it authenticates and again when it closes, which is the normal life of
    // one: a second release must not create a permit out of nothing.
    final PreAuthConnectionGate gate = new PreAuthConnectionGate("TEST", 1);

    final PreAuthConnectionGate.Ticket ticket = gate.accept();
    ticket.release();
    ticket.release();

    assertThat(gate.accept()).isNotNull();
    assertThat(gate.accept()).as("the cap is still 1, not 2").isNull();
  }

  @Test
  void aCapOfZeroMeansUnlimited() {
    final PreAuthConnectionGate gate = new PreAuthConnectionGate("TEST", 0);

    for (int i = 0; i < 1_000; i++)
      assertThat(gate.accept()).isNotNull();

    assertThat(gate.getRefused()).isZero();
    assertThat(gate.getPending()).isZero();
  }

  @Test
  void theCapHoldsUnderConcurrentAccepts() throws Exception {
    final int cap = 8;
    final int threads = 32;
    final PreAuthConnectionGate gate = new PreAuthConnectionGate("TEST", cap);

    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    final AtomicInteger accepted = new AtomicInteger();
    final List<Thread> workers = new ArrayList<>(threads);

    for (int i = 0; i < threads; i++) {
      final Thread worker = new Thread(() -> {
        try {
          start.await();
          if (gate.accept() != null)
            accepted.incrementAndGet();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          done.countDown();
        }
      });
      workers.add(worker);
      worker.start();
    }

    start.countDown();
    assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
    for (final Thread worker : workers)
      worker.join();

    assertThat(accepted.get()).isEqualTo(cap);
    assertThat(gate.getRefused()).isEqualTo(threads - cap);
  }
}
