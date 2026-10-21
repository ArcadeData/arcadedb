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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The wire format and the flush policy of the streaming query encoding (issue #7306), exercised without a
 * server: the format is a contract every client parses against, and the flush policy is the difference between
 * a stream that delivers and one that merely claims to.
 */
class NdJsonResultStreamTest {

  /**
   * Records the byte offset at which each flush happened, which is what lets a test assert that a row actually
   * left rather than only that it was written.
   */
  private static class FlushRecordingStream extends OutputStream {
    final ByteArrayOutputStream bytes    = new ByteArrayOutputStream();
    final List<Integer>         flushes  = new ArrayList<>();
    boolean                     closed;

    @Override
    public void write(final int b) {
      bytes.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) {
      bytes.write(b, off, len);
    }

    @Override
    public void flush() {
      flushes.add(bytes.size());
    }

    @Override
    public void close() {
      closed = true;
    }

    String text() {
      return bytes.toString(StandardCharsets.UTF_8);
    }

    List<String> lines() {
      final String text = text();
      return text.isEmpty() ? List.of() : List.of(text.split("\n"));
    }
  }

  @Test
  void everyLineIsAnEnvelopeNamingItsEventKind() throws IOException {
    final FlushRecordingStream out = new FlushRecordingStream();
    try (final NdJsonResultStream stream = new NdJsonResultStream(out)) {
      stream.writeRecord(new JSONObject().put("name", "a"));
      stream.writeRecord(new JSONObject().put("name", "b"));
      stream.writeStats(10, 2, false);
    }

    final List<String> lines = out.lines();
    assertThat(lines).hasSize(3);

    assertThat(new JSONObject(lines.get(0)).getJSONObject("record").getString("name")).isEqualTo("a");
    assertThat(new JSONObject(lines.get(1)).getJSONObject("record").getString("name")).isEqualTo("b");

    final JSONObject stats = new JSONObject(lines.get(2)).getJSONObject("stats");
    assertThat(stats.getInt("limit")).isEqualTo(10);
    assertThat(stats.getInt("returned")).isEqualTo(2);
    assertThat(stats.getBoolean("truncated")).isFalse();
    assertThat(out.closed).isTrue();
  }

  /**
   * A trailer written for an uncapped stream reports -1 rather than 0, matching the buffered response: a caller
   * reading 0 would conclude the server refused to return anything.
   */
  @Test
  void anUncappedStreamReportsMinusOneAsItsLimit() throws IOException {
    final FlushRecordingStream out = new FlushRecordingStream();
    try (final NdJsonResultStream stream = new NdJsonResultStream(out)) {
      stream.writeStats(0, 7, false);
    }
    assertThat(new JSONObject(out.lines().getFirst()).getJSONObject("stats").getInt("limit")).isEqualTo(-1);
  }

  @Test
  void anErrorLineCarriesItsMessageAndIsNotFollowedByATrailer() throws IOException {
    final FlushRecordingStream out = new FlushRecordingStream();
    try (final NdJsonResultStream stream = new NdJsonResultStream(out)) {
      stream.writeRecord(new JSONObject().put("name", "a"));
      stream.writeError(new JSONObject().put("message", "index rebuild in progress").put("status", 503));
    }

    final List<String> lines = out.lines();
    assertThat(lines).hasSize(2);
    assertThat(new JSONObject(lines.get(1)).getJSONObject("error").getString("message"))
        .isEqualTo("index rebuild in progress");
    assertThat(lines).noneMatch(line -> line.contains("\"stats\""));
  }

  /**
   * Issues #8235, #8899: the body the handler built from its classifier travels unchanged under the {@code error}
   * envelope - the stream adds nothing and drops nothing, so what members a line carries is decided in one place.
   */
  @Test
  void anErrorLineCarriesTheBodyItIsGivenVerbatim() throws IOException {
    final FlushRecordingStream out = new FlushRecordingStream();
    final JSONObject body = new JSONObject().put("message", "Cannot execute command").put("status", 503)
        .put("exception", "com.arcadedb.server.http.RetryLaterException").put("exceptionArgs", "7").put("retryAfter", 7L);
    try (final NdJsonResultStream stream = new NdJsonResultStream(out)) {
      stream.writeError(body);
    }

    final JSONObject line = new JSONObject(out.lines().getFirst());
    assertThat(line.keySet()).containsExactly("error");
    assertThat(line.getJSONObject("error").toString()).isEqualTo(body.toString());
  }

  /**
   * The first line always flushes. Without it a consumer would not see the stream open until the size or time
   * threshold was crossed, which for a slow query is exactly the latency the encoding exists to remove.
   */
  @Test
  void theFirstLineIsFlushedImmediately() throws IOException {
    final FlushRecordingStream out = new FlushRecordingStream();
    final NdJsonResultStream stream = new NdJsonResultStream(out, Long.MAX_VALUE / 2_000_000L);
    stream.writeRecord(new JSONObject().put("name", "a"));

    assertThat(out.flushes)
        .as("the first row must reach the client before anything else is produced")
        .hasSize(1);
    assertThat(out.flushes.getFirst()).isEqualTo(out.bytes.size());
  }

  /**
   * After the first line, small rows accumulate instead of costing a blocking write each. The time-based half of
   * the policy is disabled here (an effectively infinite interval) so the assertion is about size alone and does
   * not race a clock.
   */
  @Test
  void subsequentSmallLinesAccumulateUntilTheSizeThreshold() throws IOException {
    final FlushRecordingStream out = new FlushRecordingStream();
    final NdJsonResultStream stream = new NdJsonResultStream(out, Long.MAX_VALUE / 2_000_000L);

    // A row of a few dozen bytes: it takes many of them to reach the 8 KiB threshold.
    final JSONObject row = new JSONObject().put("name", "abcdefghij");
    for (int i = 0; i < 20; i++)
      stream.writeRecord(row);

    assertThat(out.flushes)
        .as("only the first line should have forced a write at this size")
        .hasSize(1);

    // Now cross the threshold in one go.
    final StringBuilder big = new StringBuilder();
    big.append("x".repeat(NdJsonResultStream.FLUSH_THRESHOLD_BYTES));
    stream.writeRecord(new JSONObject().put("name", big.toString()));

    assertThat(out.flushes).hasSize(2);
  }

  /**
   * The time-based half is what bounds delivery latency when the rows are small and the engine is slow. Driven
   * with a zero interval so every line is due, rather than by sleeping.
   */
  @Test
  void theTimeThresholdForcesAFlushOnASlowStream() throws IOException {
    final FlushRecordingStream out = new FlushRecordingStream();
    final NdJsonResultStream stream = new NdJsonResultStream(out, 0);

    stream.writeRecord(new JSONObject().put("name", "a"));
    stream.writeRecord(new JSONObject().put("name", "b"));
    stream.writeRecord(new JSONObject().put("name", "c"));

    assertThat(out.flushes).hasSize(3);
  }

  @Test
  void closingFlushesWhateverIsStillPending() throws IOException {
    final FlushRecordingStream out = new FlushRecordingStream();
    final NdJsonResultStream stream = new NdJsonResultStream(out, Long.MAX_VALUE / 2_000_000L);
    stream.writeRecord(new JSONObject().put("name", "a")); // flushed: first line
    stream.writeRecord(new JSONObject().put("name", "b")); // pending
    stream.close();

    assertThat(out.flushes).hasSize(2);
    assertThat(out.flushes.getLast()).isEqualTo(out.bytes.size());
    assertThat(out.closed).isTrue();
  }

  /** Issue #8565: a stream that has been silent for the interval says it is alive, and nothing else. */
  @Test
  void keepAliveWritesABareNewlineOnlyAfterTheIdleInterval() throws IOException {
    final FlushRecordingStream out = new FlushRecordingStream();
    try (final NdJsonResultStream stream = new NdJsonResultStream(out, 50)) {
      assertThat(stream.keepAlive(60_000)).as("time left until it could be idle").isBetween(1L, 60_000L);
      assertThat(out.text()).as("not idle for long enough yet").isEmpty();

      assertThat(stream.keepAlive(0)).isGreaterThanOrEqualTo(0);
      assertThat(out.text()).isEqualTo("\n");
      assertThat(out.flushes).hasSize(1);
      assertThat(stream.hasStarted()).isTrue();
    }
  }

  @Test
  void keepAliveDeliversPendingRowsInsteadOfAddingANewline() throws IOException {
    final FlushRecordingStream out = new FlushRecordingStream();
    try (final NdJsonResultStream stream = new NdJsonResultStream(out, 60_000)) {
      stream.writeRecord(new JSONObject().put("n", 1)); // first line is flushed at once
      stream.writeRecord(new JSONObject().put("n", 2)); // pending: neither threshold reached
      assertThat(out.flushes).hasSize(1);

      assertThat(stream.keepAlive(0)).isGreaterThanOrEqualTo(0);
      assertThat(out.flushes).hasSize(2);
      assertThat(out.lines()).hasSize(2).noneMatch(String::isEmpty);
    }
  }

  @Test
  void keepAliveStopsOnceTheStreamIsClosed() throws IOException {
    final FlushRecordingStream out = new FlushRecordingStream();
    final NdJsonResultStream stream = new NdJsonResultStream(out, 50);
    stream.close();

    assertThat(stream.keepAlive(0)).isEqualTo(-1);
    assertThat(out.text()).isEmpty();
  }

  @Test
  void keepAliveReportsAFailedWriteSoTheTimerStops() throws IOException {
    final OutputStream failing = new OutputStream() {
      @Override
      public void write(final int b) throws IOException {
        throw new IOException("client gone");
      }
    };
    final NdJsonResultStream stream = new NdJsonResultStream(failing, 50);

    assertThat(stream.keepAlive(0)).isEqualTo(-1);
  }

  @Test
  void keepAliveDoesNotWaitForAWriterHoldingTheStream() throws Exception {
    final CountDownLatch inWrite = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final OutputStream blocking = new OutputStream() {
      @Override
      public void write(final int b) {
      }

      @Override
      public void write(final byte[] b, final int off, final int len) {
        inWrite.countDown();
        try {
          release.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };
    final NdJsonResultStream stream = new NdJsonResultStream(blocking, 50);
    final Thread writer = new Thread(() -> {
      try {
        stream.writeRecord(new JSONObject().put("n", 1));
      } catch (final IOException ignored) {
      }
    });
    writer.start();
    try {
      assertThat(inWrite.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(stream.keepAlive(0)).as("a stream somebody is writing to is not silent").isEqualTo(0);
    } finally {
      release.countDown();
      writer.join(10_000);
    }
  }
}
