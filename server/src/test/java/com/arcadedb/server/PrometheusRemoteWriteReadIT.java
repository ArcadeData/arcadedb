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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Label;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.LabelMatcher;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.MatchType;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Query;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.QueryResult;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadRequest;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.ReadResponse;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.Sample;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.TimeSeries;
import com.arcadedb.server.http.handler.prometheus.PrometheusTypes.WriteRequest;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Prometheus remote_write and remote_read endpoints.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PrometheusRemoteWriteReadIT extends BaseGraphServerTest {

  @Test
  void testRemoteWriteBasic() throws Exception {
    testEachServer((serverIndex) -> {
      final WriteRequest writeRequest = new WriteRequest(List.of(
          new TimeSeries(
              List.of(new Label("__name__", "cpu_usage"), new Label("host", "server1")),
              List.of(new Sample(75.5, 1000), new Sample(80.2, 2000))
          ),
          new TimeSeries(
              List.of(new Label("__name__", "cpu_usage"), new Label("host", "server2")),
              List.of(new Sample(60.1, 1000))
          )
      ));

      final int statusCode = postPromWrite(serverIndex, writeRequest);
      assertThat(statusCode).isEqualTo(204);

      // Verify data via SQL
      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM cpu_usage");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(3);
    });
  }

  @Test
  void testRemoteWriteAutoCreateType() throws Exception {
    testEachServer((serverIndex) -> {
      final WriteRequest writeRequest = new WriteRequest(List.of(
          new TimeSeries(
              List.of(new Label("__name__", "disk_io"), new Label("device", "sda")),
              List.of(new Sample(1024.0, 5000))
          )
      ));

      final int statusCode = postPromWrite(serverIndex, writeRequest);
      assertThat(statusCode).isEqualTo(204);

      // Verify the type was auto-created
      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM disk_io");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(1);
    });
  }

  @Test
  void testRemoteWriteMultipleLabels() throws Exception {
    testEachServer((serverIndex) -> {
      final WriteRequest writeRequest = new WriteRequest(List.of(
          new TimeSeries(
              List.of(
                  new Label("__name__", "http_requests"),
                  new Label("method", "GET"),
                  new Label("path", "/api/v1"),
                  new Label("status", "200")
              ),
              List.of(new Sample(42.0, 3000))
          )
      ));

      final int statusCode = postPromWrite(serverIndex, writeRequest);
      assertThat(statusCode).isEqualTo(204);

      final JSONObject result = executeCommand(serverIndex, "sql", "SELECT FROM http_requests");
      assertThat(result).isNotNull();
      final JSONArray records = result.getJSONObject("result").getJSONArray("records");
      assertThat(records.length()).isEqualTo(1);
    });
  }

  @Test
  void testRemoteWriteEmptyBody() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = createPromConnection(serverIndex, "prom/write");
      connection.setDoOutput(true);
      try (final OutputStream os = connection.getOutputStream()) {
        os.flush();
      }
      assertThat(connection.getResponseCode()).isEqualTo(400);
    });
  }

  @Test
  void testRemoteWriteInvalidSnappy() throws Exception {
    testEachServer((serverIndex) -> {
      final HttpURLConnection connection = createPromConnection(serverIndex, "prom/write");
      connection.setDoOutput(true);
      try (final OutputStream os = connection.getOutputStream()) {
        os.write(new byte[] { 0x01, 0x02, 0x03, 0x04 }); // invalid snappy data
        os.flush();
      }
      assertThat(connection.getResponseCode()).isEqualTo(400);
    });
  }

  @Test
  void testRemoteReadBasic() throws Exception {
    testEachServer((serverIndex) -> {
      // First insert some data
      final WriteRequest writeRequest = new WriteRequest(List.of(
          new TimeSeries(
              List.of(new Label("__name__", "memory_usage"), new Label("host", "node1")),
              List.of(new Sample(1024.0, 1000), new Sample(2048.0, 2000), new Sample(4096.0, 3000))
          )
      ));
      final int writeStatus = postPromWrite(serverIndex, writeRequest);
      assertThat(writeStatus).isEqualTo(204);

      // Now read it back
      final ReadRequest readRequest = new ReadRequest(List.of(
          new Query(0, 5000, List.of(
              new LabelMatcher(MatchType.EQ, "__name__", "memory_usage")
          ))
      ));

      final ReadResponse readResponse = postPromRead(serverIndex, readRequest);
      assertThat(readResponse).isNotNull();
      assertThat(readResponse.getResults()).hasSize(1);

      final QueryResult qr = readResponse.getResults().getFirst();
      assertThat(qr.getTimeSeries()).hasSize(1);

      final TimeSeries ts = qr.getTimeSeries().getFirst();
      assertThat(ts.getSamples()).hasSize(3);
      assertThat(ts.getSamples().get(0).value()).isEqualTo(1024.0);
      assertThat(ts.getSamples().get(1).value()).isEqualTo(2048.0);
      assertThat(ts.getSamples().get(2).value()).isEqualTo(4096.0);
    });
  }

  @Test
  void testRemoteReadWithLabelMatcher() throws Exception {
    testEachServer((serverIndex) -> {
      // Insert data with different label values
      final WriteRequest writeRequest = new WriteRequest(List.of(
          new TimeSeries(
              List.of(new Label("__name__", "net_bytes"), new Label("iface", "eth0")),
              List.of(new Sample(100.0, 1000))
          ),
          new TimeSeries(
              List.of(new Label("__name__", "net_bytes"), new Label("iface", "eth1")),
              List.of(new Sample(200.0, 1000))
          )
      ));
      assertThat(postPromWrite(serverIndex, writeRequest)).isEqualTo(204);

      // Read only eth0
      final ReadRequest readRequest = new ReadRequest(List.of(
          new Query(0, 5000, List.of(
              new LabelMatcher(MatchType.EQ, "__name__", "net_bytes"),
              new LabelMatcher(MatchType.EQ, "iface", "eth0")
          ))
      ));

      final ReadResponse readResponse = postPromRead(serverIndex, readRequest);
      assertThat(readResponse.getResults()).hasSize(1);
      final QueryResult qr = readResponse.getResults().getFirst();
      assertThat(qr.getTimeSeries()).hasSize(1);
      assertThat(qr.getTimeSeries().getFirst().getSamples()).hasSize(1);
      assertThat(qr.getTimeSeries().getFirst().getSamples().getFirst().value()).isEqualTo(100.0);
    });
  }

  @Test
  void testRemoteReadEmptyResult() throws Exception {
    testEachServer((serverIndex) -> {
      final ReadRequest readRequest = new ReadRequest(List.of(
          new Query(0, 5000, List.of(
              new LabelMatcher(MatchType.EQ, "__name__", "nonexistent_metric")
          ))
      ));

      final ReadResponse readResponse = postPromRead(serverIndex, readRequest);
      assertThat(readResponse.getResults()).hasSize(1);
      assertThat(readResponse.getResults().getFirst().getTimeSeries()).isEmpty();
    });
  }

  @Test
  void testProtobufRoundtrip() throws Exception {
    // Test WriteRequest encode/decode
    final WriteRequest writeReq = new WriteRequest(List.of(
        new TimeSeries(
            List.of(new Label("__name__", "test_metric"), new Label("job", "test")),
            List.of(new Sample(3.14, 12345), new Sample(2.72, 12346))
        )
    ));

    final byte[] encoded = writeReq.encode();
    final WriteRequest decoded = WriteRequest.decode(encoded);
    assertThat(decoded.getTimeSeries()).hasSize(1);
    assertThat(decoded.getTimeSeries().getFirst().getLabels()).hasSize(2);
    assertThat(decoded.getTimeSeries().getFirst().getSamples()).hasSize(2);
    assertThat(decoded.getTimeSeries().getFirst().getSamples().getFirst().value()).isEqualTo(3.14);
    assertThat(decoded.getTimeSeries().getFirst().getSamples().getFirst().timestampMs()).isEqualTo(12345);
    assertThat(decoded.getTimeSeries().getFirst().getMetricName()).isEqualTo("test_metric");

    // Test ReadRequest encode/decode
    final ReadRequest readReq = new ReadRequest(List.of(
        new Query(100, 200, List.of(
            new LabelMatcher(MatchType.EQ, "__name__", "test_metric"),
            new LabelMatcher(MatchType.NEQ, "env", "dev")
        ))
    ));

    final byte[] readEncoded = readReq.encode();
    final ReadRequest readDecoded = ReadRequest.decode(readEncoded);
    assertThat(readDecoded.getQueries()).hasSize(1);
    assertThat(readDecoded.getQueries().getFirst().getStartTimestampMs()).isEqualTo(100);
    assertThat(readDecoded.getQueries().getFirst().getEndTimestampMs()).isEqualTo(200);
    assertThat(readDecoded.getQueries().getFirst().getMatchers()).hasSize(2);
    assertThat(readDecoded.getQueries().getFirst().getMatchers().get(1).type()).isEqualTo(MatchType.NEQ);

    // Test ReadResponse encode/decode
    final ReadResponse readResp = new ReadResponse(List.of(
        new QueryResult(List.of(
            new TimeSeries(
                List.of(new Label("__name__", "test_metric")),
                List.of(new Sample(99.9, 555))
            )
        ))
    ));

    final byte[] respEncoded = readResp.encode();
    final ReadResponse respDecoded = ReadResponse.decode(respEncoded);
    assertThat(respDecoded.getResults()).hasSize(1);
    assertThat(respDecoded.getResults().getFirst().getTimeSeries()).hasSize(1);
    assertThat(respDecoded.getResults().getFirst().getTimeSeries().getFirst().getSamples().getFirst().value())
        .isEqualTo(99.9);
  }

  // ---- Helpers ----

  private int postPromWrite(final int serverIndex, final WriteRequest writeRequest) throws Exception {
    final byte[] protobufBytes = writeRequest.encode();
    final byte[] compressed = Snappy.compress(protobufBytes);

    final HttpURLConnection connection = createPromConnection(serverIndex, "prom/write");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(compressed);
      os.flush();
    }

    return connection.getResponseCode();
  }

  private ReadResponse postPromRead(final int serverIndex, final ReadRequest readRequest) throws Exception {
    final byte[] protobufBytes = readRequest.encode();
    final byte[] compressed = Snappy.compress(protobufBytes);

    final HttpURLConnection connection = createPromConnection(serverIndex, "prom/read");
    connection.setDoOutput(true);
    try (final OutputStream os = connection.getOutputStream()) {
      os.write(compressed);
      os.flush();
    }

    assertThat(connection.getResponseCode()).isEqualTo(200);

    // Read binary response
    try (final InputStream is = connection.getInputStream()) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final byte[] buf = new byte[4096];
      int n;
      while ((n = is.read(buf)) != -1)
        baos.write(buf, 0, n);
      final byte[] responseCompressed = baos.toByteArray();
      final byte[] responseBytes = Snappy.uncompress(responseCompressed);
      return ReadResponse.decode(responseBytes);
    }
  }

  private HttpURLConnection createPromConnection(final int serverIndex, final String path) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/" + path)
        .toURL()
        .openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/x-protobuf");
    connection.setRequestProperty("Content-Encoding", "snappy");
    return connection;
  }
}
