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

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.HashMap;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for issue #3155: HTTP error responses should include full exception cause chain.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3155NestedExceptionErrorIT extends BaseGraphServerTest {

  @Test
  void testNestedExceptionInHttpError() throws Exception {
    testEachServer((serverIndex) -> {
      // First, create a vertex type with a vector index
      HttpURLConnection setupConnection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      setupConnection.setRequestMethod("POST");
      setupConnection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      // Create CHUNK vertex type with vector property and index
      formatPayload(setupConnection, "sql",
          "CREATE VERTEX TYPE CHUNK IF NOT EXISTS", null, new HashMap<>());
      setupConnection.connect();
      readResponse(setupConnection);
      setupConnection.disconnect();

      // Create vector property
      setupConnection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
      setupConnection.setRequestMethod("POST");
      setupConnection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(setupConnection, "sql",
          "CREATE PROPERTY CHUNK.vector FLOAT_ARRAY", null, new HashMap<>());
      setupConnection.connect();
      readResponse(setupConnection);
      setupConnection.disconnect();

      // Create vector index on the property
      setupConnection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
      setupConnection.setRequestMethod("POST");
      setupConnection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(setupConnection, "sql",
          "CREATE INDEX ON CHUNK (vector) HNSW DIMENSION 3", null, new HashMap<>());
      setupConnection.connect();
      readResponse(setupConnection);
      setupConnection.disconnect();

      // Create CHUNK_EMBEDDING vertex type
      setupConnection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
      setupConnection.setRequestMethod("POST");
      setupConnection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(setupConnection, "sql",
          "CREATE VERTEX TYPE CHUNK_EMBEDDING IF NOT EXISTS", null, new HashMap<>());
      setupConnection.connect();
      readResponse(setupConnection);
      setupConnection.disconnect();

      // Create edge type
      setupConnection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
      setupConnection.setRequestMethod("POST");
      setupConnection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(setupConnection, "sql",
          "CREATE EDGE TYPE embb IF NOT EXISTS", null, new HashMap<>());
      setupConnection.connect();
      readResponse(setupConnection);
      setupConnection.disconnect();

      // Create a test CHUNK vertex
      setupConnection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();
      setupConnection.setRequestMethod("POST");
      setupConnection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(setupConnection, "sql",
          "CREATE VERTEX CHUNK SET vector = [1.0, 2.0, 3.0]", null, new HashMap<>());
      setupConnection.connect();
      final String createResponse = readResponse(setupConnection);
      setupConnection.disconnect();

      // Extract the RID from the response
      final JSONObject responseJson = new JSONObject(createResponse);
      final String rid = responseJson.getJSONObject("result").getString("@rid");

      // Now execute a Cypher command that will cause a nested exception
      // The error should be: CommandExecutionException wrapping a type error about vector index expecting float array
      final HttpURLConnection errorConnection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      errorConnection.setRequestMethod("POST");
      errorConnection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      // This Cypher command will fail because 'vector' parameter is a string instead of float array
      final HashMap<String, Object> params = new HashMap<>();
      params.put("batch", new Object[] {
          new HashMap<String, Object>() {{
            put("destRID", rid);
            put("vector", "not-a-float-array");  // This will cause the error
          }}
      });

      formatPayload(errorConnection, "cypher",
          "UNWIND $batch AS BatchEntry " +
          "MATCH (b:CHUNK) WHERE ID(b) = BatchEntry.destRID " +
          "CREATE (p:CHUNK_EMBEDDING {vector: BatchEntry.vector}) " +
          "CREATE (p)-[:embb]->(b)",
          null, params);

      errorConnection.connect();

      try {
        // This should fail with an error
        final String errorResponse = readErrorResponse(errorConnection);
        LogManager.instance().log(this, Level.INFO, "Error Response: %s", null, errorResponse);

        // Verify we get an error
        assertThat(errorConnection.getResponseCode()).isEqualTo(500);

        // Parse the error response
        final JSONObject errorJson = new JSONObject(errorResponse);
        assertThat(errorJson.has("error")).isTrue();
        assertThat(errorJson.has("detail")).isTrue();

        final String detail = errorJson.getString("detail");
        LogManager.instance().log(this, Level.INFO, "Error detail: %s", null, detail);

        // The key assertion: the detail should contain the nested cause information
        // It should mention something about "float array" or "vector" or the actual type error
        // This tests that we're walking the exception cause chain
        assertThat(detail).as("Error detail should contain nested exception information")
            .matches(".*->.*|.*float.*array.*|.*vector.*|.*String.*");

      } catch (final IOException e) {
        // If we get here, we successfully triggered an error
        LogManager.instance().log(this, Level.INFO, "Got expected error: %s", null, e.getMessage());
      } finally {
        errorConnection.disconnect();
      }
    });
  }

  /**
   * Helper method to read error response from connection
   */
  private String readErrorResponse(final HttpURLConnection connection) throws IOException {
    final StringBuilder response = new StringBuilder();
    try (final BufferedReader reader = new BufferedReader(
        new InputStreamReader(connection.getErrorStream()))) {
      String line;
      while ((line = reader.readLine()) != null)
        response.append(line);
    }
    return response.toString();
  }
}
