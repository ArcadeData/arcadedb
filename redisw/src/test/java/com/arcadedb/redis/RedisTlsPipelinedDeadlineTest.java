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
package com.arcadedb.redis;

import com.arcadedb.server.network.PreAuthConnectionGate;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A pipelined TLS connection to the Redis listener that has not authenticated is closed at the pre-authentication deadline,
 * and the deadline of every other connection of the JVM keeps being applied afterwards.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class RedisTlsPipelinedDeadlineTest extends RedisTlsPreAuthTestBase {

  private static final long DEADLINE_MS = 8_000;

  @Override
  long deadlineMs() {
    return DEADLINE_MS;
  }

  @Test
  void aPipelinedTlsConnectionIsClosedAtTheDeadline() throws Exception {
    final SSLSocket client = connectTls();

    final AtomicLong sent = new AtomicLong();
    final CountDownLatch writerEnded = new CountDownLatch(1);
    final Thread writer = new Thread(() -> {
      final byte[] batch = "PING\r\n".repeat(8_192).getBytes(StandardCharsets.ISO_8859_1);
      try {
        final OutputStream out = client.getOutputStream();
        while (true) {
          out.write(batch);
          out.flush();
          sent.addAndGet(batch.length);
        }
      } catch (final IOException e) {
        // the server closed the connection
      } finally {
        writerEnded.countDown();
      }
    }, "redis-tls-test-writer");
    writer.setDaemon(true);
    writer.start();

    // the writer has no more room to send, and the connection is open
    awaitNoProgress(sent, writerEnded);
    assertThat(writerEnded.getCount()).as("the connection is still open before the deadline").isEqualTo(1);

    // a hang detector, not a latency bound: the reaper ticks every second
    assertThat(writerEnded.await(DEADLINE_MS + 30_000, TimeUnit.MILLISECONDS)).as("the connection is closed at the deadline")
        .isTrue();

    // the deadline of the connection of another gate is applied afterwards
    final PreAuthConnectionGate gate = new PreAuthConnectionGate("TEST", 1, 500);
    try (final Socket other = new Socket()) {
      assertThat(gate.accept(other)).isNotNull();

      final long giveUpAt = System.currentTimeMillis() + 60_000;
      while (!other.isClosed() && System.currentTimeMillis() < giveUpAt)
        Thread.sleep(50);

      assertThat(other.isClosed()).as("the connection of another gate is closed at its deadline").isTrue();
      assertThat(gate.getExpired()).isEqualTo(1);
    }
  }

  /** Waits until {@code progress} has not moved for a while, or {@code ended} is counted down. */
  private static void awaitNoProgress(final AtomicLong progress, final CountDownLatch ended) throws InterruptedException {
    final long giveUpAt = System.currentTimeMillis() + 30_000;
    long last = -1;
    int still = 0;
    while (ended.getCount() > 0 && System.currentTimeMillis() < giveUpAt) {
      Thread.sleep(150);
      final long now = progress.get();
      still = now > 0 && now == last ? still + 1 : 0;
      if (still == 2)
        return;
      last = now;
    }
  }
}
