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

import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.Test;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

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

  @Test
  void aConnectionStillUnauthenticatedAtTheDeadlineIsClosedAndItsPermitReturned() throws Exception {
    final PreAuthConnectionGate gate = new PreAuthConnectionGate("TEST", 1, 500);
    final Socket socket = new Socket();

    final PreAuthConnectionGate.Ticket ticket = gate.accept(socket);
    assertThat(ticket).isNotNull();
    assertThat(gate.accept(new Socket())).as("the cap is 1").isNull();

    // a hang detector, not a latency bound: the reaper ticks every second
    final long giveUpAt = System.currentTimeMillis() + 30_000;
    while (!socket.isClosed() && System.currentTimeMillis() < giveUpAt)
      Thread.sleep(50);

    assertThat(socket.isClosed()).isTrue();
    assertThat(ticket.isReleased()).isTrue();
    assertThat(gate.getExpired()).isEqualTo(1);
    assertThat(gate.getPending()).isZero();
    assertThat(gate.accept(new Socket())).as("the reclaimed permit lets the next connection in").isNotNull();
  }

  @Test
  void anAuthenticatedConnectionIsNeverClosedByTheDeadline() throws Exception {
    final PreAuthConnectionGate gate = new PreAuthConnectionGate("TEST", 1, 200);
    final Socket socket = new Socket();

    gate.accept(socket).release();
    Thread.sleep(2_500);

    assertThat(socket.isClosed()).isFalse();
    assertThat(gate.getExpired()).isZero();
    socket.close();
  }

  @Test
  void aDeadlineOfZeroClosesNothing() throws Exception {
    final PreAuthConnectionGate gate = new PreAuthConnectionGate("TEST", 1, 0);
    final Socket socket = new Socket();

    gate.accept(socket);
    Thread.sleep(1_500);

    assertThat(socket.isClosed()).isFalse();
    socket.close();
  }

  @Test
  void protocolErrorsOfUnauthenticatedConnectionsAreLoggedAtMostOncePerInterval() {
    final PreAuthConnectionGate gate = new PreAuthConnectionGate("TEST", 0);
    final AtomicInteger records = new AtomicInteger();
    final Object source = new Object();
    LogManager.instance().setLogger(countingLogger(source, records));
    try {
      for (int i = 0; i < 1_000; i++)
        gate.accept().logProtocolError(source, "junk %d", i);
      assertThat(records.get()).isEqualTo(1);
      assertThat(gate.getPreAuthProtocolErrors()).isEqualTo(1_000);

      // after authentication an error is the client's own and is logged as such
      final PreAuthConnectionGate.Ticket authenticated = gate.accept();
      authenticated.release();
      authenticated.logProtocolError(source, "junk after auth");
      assertThat(records.get()).isEqualTo(2);
    } finally {
      // process-wide: put the default back, or every later log-asserting test in this fork captures nothing
      LogManager.instance().setLogger(new DefaultLogger());
    }
  }

  private static Logger countingLogger(final Object source, final AtomicInteger records) {
    final Logger delegate = new DefaultLogger();
    return new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
          final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
          final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
          final Object arg17) {
        if (requester == source)
          records.incrementAndGet();
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        if (requester == source)
          records.incrementAndGet();
      }

      @Override
      public boolean isLoggable(final Object requester, final Level level) {
        return delegate.isLoggable(requester, level);
      }

      @Override
      public void flush() {
        delegate.flush();
      }
    };
  }
}
