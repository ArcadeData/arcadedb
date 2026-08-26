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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for InfluxDB Line Protocol ingestion via HTTP.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostTimeSeriesWriteHandlerIT extends BaseGraphServerTest {

  @Test
  void ingestLineProtocol() throws Exception {
    testEachServer(serverIndex -> {
      // Create a TimeSeries type
      command(serverIndex,
          "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");

      // Post InfluxDB Line Protocol data
      final String lineProtocol = """
          weather,location=us-east temperature=22.5 1000
          weather,location=us-west temperature=18.3 2000
          weather,location=us-east temperature=23.1 3000
          """;

      final int statusCode = postLineProtocol(serverIndex, lineProtocol, "ms");
      assertThat(statusCode).isEqualTo(204);

      // Verify data was inserted
      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM weather");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(3);
    });
  }

  @Test
  void ingestWithNanoPrecision() throws Exception {
    testEachServer(serverIndex -> {
      command(serverIndex,
          "CREATE TIMESERIES TYPE cpu TIMESTAMP ts TAGS (host STRING) FIELDS (usage DOUBLE)");

      // Nanosecond timestamps
      final String lineProtocol = "cpu,host=server1 usage=55.3 1000000000\ncpu,host=server2 usage=72.1 2000000000\n";

      final int statusCode = postLineProtocol(serverIndex, lineProtocol, "ns");
      assertThat(statusCode).isEqualTo(204);

      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM cpu");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(2);
    });
  }

  @Test
  void emptyBody() throws Exception {
    testEachServer(serverIndex -> {
      final int statusCode = postLineProtocol(serverIndex, "", "ms");
      assertThat(statusCode).isEqualTo(400);
    });
  }

  @Test
  void unknownMeasurementTypeReturnsError() throws Exception {
    // When all samples reference measurement types that have no matching TIMESERIES TYPE,
    // the handler must NOT silently return 204 - that hides data loss from the caller.
    testEachServer(serverIndex -> {
      final String lineProtocol = "ghost_metric,host=srv1 value=1.0 1000\n";
      final int statusCode = postLineProtocol(serverIndex, lineProtocol, "ms");
      assertThat(statusCode).isEqualTo(400);
    });
  }

  @Test
  void nonTimeSeriesTypeReturnsError() throws Exception {
    // When all samples reference a type that exists but is NOT a TIMESERIES type,
    // the handler must return 400 - not silently return 204 with zero rows written.
    testEachServer(serverIndex -> {
      command(serverIndex, "CREATE DOCUMENT TYPE plain_doc");
      final String lineProtocol = "plain_doc,host=srv1 value=1.0 1000\n";
      final int statusCode = postLineProtocol(serverIndex, lineProtocol, "ms");
      assertThat(statusCode).isEqualTo(400);
    });
  }

  /**
   * Regression for issue #6356's follow-up (claude-review on PR #6779): a TimeSeries type IS the right kind of
   * type, it just failed to load its storage - conflating that with "wrong type" sent an operator debugging via
   * this endpoint chasing the wrong cause. Reuses the {@code flipByteAt} + close/reopen reproduction already
   * established at the engine level in {@code Issue6340TimeSeriesCheckDatabaseTest}, driven through the actual
   * HTTP write path this time.
   */
  @Test
  void engineUnavailableTypeIsReportedDistinctlyFromNonTimeSeriesType() throws Exception {
    testEachServer(serverIndex -> {
      command(serverIndex,
          "CREATE TIMESERIES TYPE broken TIMESTAMP ts TAGS (host STRING) FIELDS (usage DOUBLE)");
      command(serverIndex, "INSERT INTO broken SET ts = 1700000000000, host = 'h', usage = 1.0");

      final ArcadeDBServer server = getServer(serverIndex);
      final DatabaseInternal embedded = (DatabaseInternal) server.getDatabase(getDatabaseName()).getEmbedded();
      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) embedded.getSchema().getType("broken");
      final TimeSeriesEngine engine = tsType.getEngine();
      engine.compactAll();

      final File sealed = new File(getDatabasePath(serverIndex), "broken_shard_0.ts.sealed");
      assertThat(sealed).exists();

      // Close and reopen the database from disk, byte-flipped, so the corruption is discovered on load exactly
      // as it would be after a real restart - not by poking the in-memory schema directly.
      embedded.close();
      server.removeDatabase(getDatabaseName());
      try (final RandomAccessFile raf = new RandomAccessFile(sealed, "rw")) {
        raf.seek(0);
        final int b = raf.read();
        raf.seek(0);
        raf.write(b ^ 0x01);
      }
      final DatabaseInternal reopened = (DatabaseInternal) server.getDatabase(getDatabaseName()).getEmbedded();
      final LocalTimeSeriesType reopenedType = (LocalTimeSeriesType) reopened.getSchema().getType("broken");
      assertThat(reopenedType.isEngineAvailable()).as("the type must stay registered, just without a usable engine")
          .isFalse();

      final HttpURLConnection connection = openWriteConnection(serverIndex, "ms");
      try (final OutputStream os = connection.getOutputStream()) {
        os.write("broken,host=h2 usage=2.0 2000\n".getBytes(StandardCharsets.UTF_8));
        os.flush();
      }
      assertThat(connection.getResponseCode()).isEqualTo(400);

      final JSONObject error = new JSONObject(readError(connection));
      assertThat(error.getString("error")).as("must be distinguishable from \"is not a TimeSeries type\"")
          .contains("no storage engine available");
      assertThat(error.getJSONArray("unavailableTypes").getString(0)).isEqualTo("broken");
      assertThat(error.has("nonTimeSeriesTypes")).as("a broken type must not also be reported as the wrong type")
          .isFalse();
    });
  }

  @Test
  void partialWriteReportsDroppedMeasurements() throws Exception {
    // Regression for issue #5036: an ingest mixing a valid TIMESERIES measurement with an unknown one
    // must NOT return 204. The valid sample is persisted (partial write) but the response must be a
    // 400 partial-write payload naming the dropped measurement, so the client knows data was discarded.
    testEachServer(serverIndex -> {
      command(serverIndex,
          "CREATE TIMESERIES TYPE pw_known TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");

      final String lineProtocol = """
          pw_known,location=us temperature=22.5 1000
          pw_unknown,location=us value=1.0 2000
          """;

      final HttpURLConnection connection = openWriteConnection(serverIndex, "ms");
      try (final OutputStream os = connection.getOutputStream()) {
        os.write(lineProtocol.getBytes(StandardCharsets.UTF_8));
        os.flush();
      }

      assertThat(connection.getResponseCode()).isEqualTo(400);

      final JSONObject error = new JSONObject(readError(connection));
      assertThat(error.getString("error")).contains("pw_unknown");
      assertThat(error.getInt("written")).isEqualTo(1);
      assertThat(error.getInt("dropped")).isEqualTo(1);

      // The valid sample must have been persisted despite the partial-write error.
      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM pw_known");
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(1);
    });
  }

  @Test
  void allDroppedReportsPartialWritePayloadShape() throws Exception {
    // Regression for issue #5036: the all-dropped case (written=0) keeps returning 400 and now carries
    // the same partial-write payload shape (written/dropped/unknownTypes) as the mixed case.
    testEachServer(serverIndex -> {
      final String lineProtocol = "ghost_only,host=srv1 value=1.0 1000\n";

      final HttpURLConnection connection = openWriteConnection(serverIndex, "ms");
      try (final OutputStream os = connection.getOutputStream()) {
        os.write(lineProtocol.getBytes(StandardCharsets.UTF_8));
        os.flush();
      }

      assertThat(connection.getResponseCode()).isEqualTo(400);

      final JSONObject error = new JSONObject(readError(connection));
      assertThat(error.getInt("written")).isEqualTo(0);
      assertThat(error.getInt("dropped")).isEqualTo(1);
      assertThat(error.getJSONArray("unknownTypes").getString(0)).isEqualTo("ghost_only");
    });
  }

  @Test
  void gzipCompressedBodyIsAccepted() throws Exception {
    // Telegraf's [[outputs.influxdb]] plugin sends Content-Encoding: gzip by default.
    // The write handler must decompress the body before parsing it.
    testEachServer(serverIndex -> {
      command(serverIndex,
          "CREATE TIMESERIES TYPE disk TIMESTAMP ts TAGS (host STRING) FIELDS (used DOUBLE)");

      final String lineProtocol = "disk,host=server1 used=42.0 1000000000\ndisk,host=server2 used=77.5 2000000000\n";

      final byte[] compressed;
      try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
          final GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
        gzip.write(lineProtocol.getBytes(StandardCharsets.UTF_8));
        gzip.finish();
        compressed = baos.toByteArray();
      }

      final int statusCode = postLineProtocolGzip(serverIndex, compressed, "ns");
      assertThat(statusCode).isEqualTo(204);

      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM disk");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(2);
    });
  }

  private int postLineProtocol(final int serverIndex, final String body, final String precision) throws Exception {
    final HttpURLConnection connection = openWriteConnection(serverIndex, precision);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    return connection.getResponseCode();
  }

  private HttpURLConnection openWriteConnection(final int serverIndex, final String precision) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/write?precision=" + precision)
        .toURL()
        .openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "text/plain");
    connection.setDoOutput(true);
    return connection;
  }

  private int postLineProtocolGzip(final int serverIndex, final byte[] compressedBody, final String precision) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/write?precision=" + precision)
        .toURL()
        .openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "text/plain");
    connection.setRequestProperty("Content-Encoding", "gzip");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(compressedBody);
      os.flush();
    }

    return connection.getResponseCode();
  }
}
