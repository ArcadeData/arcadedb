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

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end HTTP test for #4135: int8 query vectors via {@code $bytes} / {@code $int8} markers. */
class Int8VectorHttpIT extends BaseGraphServerTest {
  private static final int    DIMENSIONS  = 32;
  private static final int    NUM_VECTORS = 16;
  private static final String INDEX_TYPE  = "Doc";

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    db.transaction(() -> {
      final DocumentType docType = db.getSchema().createDocumentType(INDEX_TYPE);
      docType.createProperty("id", Type.INTEGER);
      docType.createProperty("embedding", Type.BINARY);

      db.getSchema()
          .buildTypeIndex(INDEX_TYPE, new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withSimilarity("COSINE")
          .withEncoding("INT8")
          .create();

      for (int i = 0; i < NUM_VECTORS; i++) {
        final MutableDocument doc = db.newDocument(INDEX_TYPE);
        doc.set("id", i);
        doc.set("embedding", quantize(generateNormalizedTestVector(DIMENSIONS, i)));
        doc.save();
      }
    });
  }

  @Test
  void neighborsAcceptsBytesMarkerOverHttp() throws Exception {
    final byte[] queryBytes = quantize(generateNormalizedTestVector(DIMENSIONS, 0));
    final String b64 = Base64.getEncoder().encodeToString(queryBytes);

    final JSONObject params = new JSONObject();
    params.put("q", new JSONObject().put("$bytes", b64));

    final JSONObject body = postQuery(
        "SELECT expand(`vector.neighbors`('" + INDEX_TYPE + "[embedding]', :q, 5))",
        params);

    final JSONArray hits = body.getJSONArray("result");
    assertThat(hits.length()).isGreaterThan(0);
    assertThat(hits.getJSONObject(0).getInt("id")).isEqualTo(0);
  }

  @Test
  void neighborsAcceptsInt8MarkerOverHttp() throws Exception {
    final byte[] queryBytes = quantize(generateNormalizedTestVector(DIMENSIONS, 0));

    final JSONArray int8Array = new JSONArray();
    for (final byte b : queryBytes)
      int8Array.put((int) b);

    final JSONObject params = new JSONObject();
    params.put("q", new JSONObject().put("$int8", int8Array));

    final JSONObject body = postQuery(
        "SELECT expand(`vector.neighbors`('" + INDEX_TYPE + "[embedding]', :q, 5))",
        params);

    final JSONArray hits = body.getJSONArray("result");
    assertThat(hits.length()).isGreaterThan(0);
    assertThat(hits.getJSONObject(0).getInt("id")).isEqualTo(0);
  }

  private JSONObject postQuery(final String command, final JSONObject params) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:2480/api/v1/query/" + getDatabaseName()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final JSONObject payload = new JSONObject();
    payload.put("language", "sql");
    payload.put("command", command);
    payload.put("params", params);

    connection.setDoOutput(true);
    final byte[] data = payload.toString().getBytes(StandardCharsets.UTF_8);
    connection.setRequestProperty("Content-Length", Integer.toString(data.length));
    try (final DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
      wr.write(data);
    }
    connection.connect();
    try {
      final int status = connection.getResponseCode();
      if (status != 200) {
        final String err = connection.getErrorStream() != null
            ? new String(connection.getErrorStream().readAllBytes(), StandardCharsets.UTF_8)
            : "<no error stream>";
        throw new AssertionError("HTTP " + status + " from " + connection.getURL() + ": " + err);
      }
      final String response = readResponse(connection);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }

  /**
   * Quantizes a {@code float} vector to signed int8 using the Cohere/OpenAI calibration convention
   * ({@code round(v * 127)}, clamped to {@code [-127, 127]}). Mirrors
   * {@code VectorUtils.dequantizeInt8ToFloat} so the round trip is lossless within byte resolution.
   */
  private static byte[] quantize(final float[] v) {
    final byte[] out = new byte[v.length];
    for (int i = 0; i < v.length; i++) {
      final int q = Math.round(v[i] * 127f);
      out[i] = (byte) Math.max(-127, Math.min(127, q));
    }
    return out;
  }

  private static float[] generateNormalizedTestVector(final int dimensions, final int seed) {
    final float[] vector = new float[dimensions];
    for (int i = 0; i < dimensions; i++)
      vector[i] = (float) Math.sin(seed + i * 0.1);
    float norm = 0f;
    for (final float x : vector)
      norm += x * x;
    norm = (float) Math.sqrt(norm);
    if (norm > 0f)
      for (int i = 0; i < dimensions; i++)
        vector[i] /= norm;
    return vector;
  }
}
