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
      stream.writeError("index rebuild in progress");
    }

    final List<String> lines = out.lines();
    assertThat(lines).hasSize(2);
    assertThat(new JSONObject(lines.get(1)).getJSONObject("error").getString("message"))
        .isEqualTo("index rebuild in progress");
    assertThat(lines).noneMatch(line -> line.contains("\"stats\""));
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
}
