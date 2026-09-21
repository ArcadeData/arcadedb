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
package com.arcadedb.server;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8084: {@code arcadedb.server.httpBodyContentMaxSize} bounds the bytes that ARRIVE, so on a route that
 * decodes a {@code Content-Encoding} it was a compression-ratio multiplier rather than a limit.
 * <p>
 * Two ingest routes decode a compressed body and materialize the whole decoded result in heap with nothing
 * bounding its size: the InfluxDB line-protocol endpoint reads gzip through
 * {@code GZIPInputStream.readAllBytes()}, and the Prometheus remote_write endpoint calls
 * {@code Snappy.uncompress}, which allocates its output array up front from a length the payload itself declares.
 * Line protocol is repetitive text and therefore close to the best case for DEFLATE - ratios in the hundreds are
 * ordinary and a crafted body does far better - so at the default 100MB wire cap an accepted request was worth
 * tens of GB of heap, on an authenticated route that answers 204 for a body it cannot even parse.
 * <p>
 * The bodies below are TINY on the wire, which is the whole point: each one passes the wire cap with room to
 * spare and is refused only because of what it expands to. The budget is lowered at runtime rather than the
 * bodies being made enormous, so the test costs milliseconds and still exercises the same decision.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8084DecompressedBodySizeLimitIT extends BaseGraphServerTest {

  /** Small enough that a few kilobytes of decoded body passes it, large enough that nothing else trips on it. */
  private static final long DECOMPRESSED_LIMIT = 64 * 1024L;

  private long previousLimit;

  @BeforeEach
  void lowerTheDecompressedBudget() {
    previousLimit = getServer(0).getConfiguration()
        .getValueAsLong(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_DECOMPRESSED_MAX_SIZE);
    getServer(0).getConfiguration()
        .setValue(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_DECOMPRESSED_MAX_SIZE, DECOMPRESSED_LIMIT);
  }

  @AfterEach
  void restoreTheDecompressedBudget() {
    getServer(0).getConfiguration()
        .setValue(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_DECOMPRESSED_MAX_SIZE, previousLimit);
  }

  /**
   * The gzip route. A body of a few hundred bytes on the wire that decodes to megabytes must be refused with 413
   * rather than buffered - and the refusal has to name the setting, because a caller that cannot see which knob
   * refused it has been told nothing it can act on.
   */
  @Test
  void aGzipLineProtocolBodyThatExpandsPastTheBudgetIsRefused() throws Exception {
    final byte[] bomb = gzip(repeatedLineProtocol(4 * 1024 * 1024));
    assertThat(bomb.length)
        .as("the body must be small on the wire, or the wire cap would be doing the refusing")
        .isLessThan((int) DECOMPRESSED_LIMIT);

    final HttpURLConnection connection = post("write?precision=ms", "text/plain", "gzip");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(bomb);
    }

    assertThat(connection.getResponseCode()).isEqualTo(413);
    assertThat(errorBody(connection))
        .contains(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_DECOMPRESSED_MAX_SIZE.getKey())
        .contains(String.valueOf(DECOMPRESSED_LIMIT));
  }

  /** A gzip body that stays inside the budget is still ingested, so the bound refuses bombs and not clients. */
  @Test
  void aGzipLineProtocolBodyInsideTheBudgetIsStillAccepted() throws Exception {
    final HttpURLConnection connection = post("write?precision=ms", "text/plain", "gzip");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(gzip("ts8084,host=h1 usage=1.0 1700000000000\n".getBytes(StandardCharsets.UTF_8)));
    }

    // 204 when every sample was written, 400 when the type does not exist yet - either way the body was DECODED
    // and parsed rather than refused, which is what this arm is about.
    assertThat(connection.getResponseCode()).isIn(204, 400);
  }

  /**
   * The Snappy route. {@code Snappy.uncompress} sizes its output array from the length the payload declares, so
   * the refusal happens before the allocation rather than after it.
   */
  @Test
  void aSnappyRemoteWriteBodyThatExpandsPastTheBudgetIsRefused() throws Exception {
    final WriteRequest request = new WriteRequest(List.of(new TimeSeries(
        List.of(new Label("__name__", "ts8084_metric"), new Label("host", "h".repeat(200_000))),
        List.of(new Sample(1.0, 1_700_000_000_000L)))));
    final byte[] bomb = Snappy.compress(request.encode());
    assertThat(bomb.length)
        .as("the body must be small on the wire, or the wire cap would be doing the refusing")
        .isLessThan((int) DECOMPRESSED_LIMIT);

    final HttpURLConnection connection = post("prom/write", "application/x-protobuf", "snappy");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(bomb);
    }

    assertThat(connection.getResponseCode()).isEqualTo(413);
    assertThat(errorBody(connection))
        .contains(GlobalConfiguration.SERVER_HTTP_BODY_CONTENT_DECOMPRESSED_MAX_SIZE.getKey());
  }

  /** The remote_READ route decodes a body exactly as the write side does, so it carries the same bound. */
  @Test
  void aSnappyRemoteReadBodyThatExpandsPastTheBudgetIsRefused() throws Exception {
    final byte[] bomb = Snappy.compress(new byte[512 * 1024]);
    assertThat(bomb.length).isLessThan((int) DECOMPRESSED_LIMIT);

    final HttpURLConnection connection = post("prom/read", "application/x-protobuf", "snappy");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(bomb);
    }

    assertThat(connection.getResponseCode()).isEqualTo(413);
  }

  /** A Snappy body inside the budget is still decoded and answered normally. */
  @Test
  void aSnappyRemoteWriteBodyInsideTheBudgetIsStillAccepted() throws Exception {
    final WriteRequest request = new WriteRequest(List.of(new TimeSeries(
        List.of(new Label("__name__", "ts8084_small"), new Label("host", "h1")),
        List.of(new Sample(1.0, 1_700_000_000_000L)))));

    final HttpURLConnection connection = post("prom/write", "application/x-protobuf", "snappy");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(Snappy.compress(request.encode()));
    }

    assertThat(connection.getResponseCode()).isEqualTo(204);
  }

  // ---- Helpers ----

  private HttpURLConnection post(final String path, final String contentType, final String contentEncoding)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:" + getServerHttpPort() + "/api/v1/ts/" + getDatabaseName() + "/" + path)
        .toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    connection.setRequestProperty("Content-Type", contentType);
    connection.setRequestProperty("Content-Encoding", contentEncoding);
    return connection;
  }

  private static String errorBody(final HttpURLConnection connection) throws Exception {
    try (final var error = connection.getErrorStream()) {
      return error == null ? "" : new String(error.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /** Line protocol repeated until it is at least {@code size} bytes: text a DEFLATE stream compresses superbly. */
  private static byte[] repeatedLineProtocol(final int size) {
    final StringBuilder builder = new StringBuilder(size + 64);
    while (builder.length() < size)
      builder.append("ts8084,host=h1,region=eu usage=1.0 1700000000000\n");
    return builder.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] gzip(final byte[] raw) throws Exception {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (final GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(raw);
    }
    return out.toByteArray();
  }
}
