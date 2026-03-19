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

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for issue #3318: Large content via HTTP problem with Undertow 2.3.22.Final
 * <p>
 * This test reproduces the bug where sending large HTTP POST bodies (>2MB) causes
 * "broken pipe" errors after upgrading from Undertow 2.3.20.Final to 2.3.22.Final.
 * <p>
 * The root cause is missing MAX_ENTITY_SIZE configuration in the Undertow builder,
 * which became stricter after CVE-2024-3884 and CVE-2024-4027 fixes in version 2.3.21.
 */
class PostCommandHandlerLargeContentTest extends BaseGraphServerTest {

  /**
   * Reproduces issue #3318 by sending a command with a large content body (~2MB).
   * <p>
   * This test creates a document type and inserts a document with a large text field
   * containing approximately 2MB of random base64-encoded data.
   */
  @Test
  void largeContentViaHTTP() throws Exception {
    // Step 1: Create document type
    executeCommand(0, "sql", "CREATE DOCUMENT TYPE doc");

    // Step 2: Generate large content (~2.7MB of base64 data)
    // This matches the issue: "dd if=/dev/urandom bs=2M count=1 | base64 -w0"
    // 2MB of random bytes = 2,097,152 bytes, base64-encoded = ~2,796,203 bytes
    final int dataSize = 2 * 1024 * 1024; // 2MB of "random" data
    final byte[] randomData = new byte[dataSize];

    // Generate pseudo-random but deterministic data for reproducibility
    for (int i = 0; i < dataSize; i++) {
      randomData[i] = (byte) ((i * 7 + 11) % 256); // Simple deterministic pattern
    }

    final String base64Data = Base64.getEncoder().encodeToString(randomData);

    logInfo("Generated test data: %d bytes (base64-encoded: %d bytes)", randomData.length, base64Data.length());
    assertThat(base64Data.length()).isGreaterThanOrEqualTo(2_700_000); // Ensure we're testing ~2.7MB

    // Step 3: Insert document with large content
    final String command = "INSERT INTO doc CONTENT {'txt':'" + base64Data + "'}";

    // Execute via HTTP POST to test the actual issue
    final JSONObject response = executeCommandViaHTTP(0, "sqlscript", command);

    // Step 4: Verify the operation succeeded
    assertThat(response).isNotNull();
    assertThat(response.toString()).contains("\"result\"");

    logInfo("Successfully inserted document with large content");

    // Step 5: Verify we can query the data back
    final JSONObject queryResponse = executeCommand(0, "sql", "SELECT FROM doc");
    assertThat(queryResponse).isNotNull();
    assertThat(queryResponse.toString()).contains("\"result\"");

    logInfo("Successfully queried document with large content");
  }

  /**
   * Helper method to execute commands via raw HTTP POST (not using the convenience method).
   * This ensures we're testing the actual HTTP layer that was failing.
   */
  private JSONObject executeCommandViaHTTP(final int serverIndex, final String language, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/command/" + getDatabaseName()).openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    // Build JSON payload
    final JSONObject jsonRequest = new JSONObject();
    jsonRequest.put("language", language);
    jsonRequest.put("command", command);

    final byte[] data = jsonRequest.toString().getBytes(StandardCharsets.UTF_8);
    logInfo("Sending HTTP POST with payload size: %d bytes", data.length);

    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setRequestProperty("Content-Length", Integer.toString(data.length));

    try (final DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
      wr.write(data);
      wr.flush();
    }

    connection.connect();

    try {
      final int responseCode = connection.getResponseCode();
      logInfo("HTTP Response Code: %d", responseCode);

      if (responseCode == 200) {
        final String response = FileUtils.readStreamAsString(connection.getInputStream(), "UTF-8");
        logInfo("Response: %s", response.substring(0, Math.min(200, response.length())) + "...");
        return new JSONObject(response);
      } else {
        final String errorResponse = connection.getErrorStream() != null
            ? FileUtils.readStreamAsString(connection.getErrorStream(), "UTF-8")
            : "No error details available";
        logError("HTTP request failed with code %d: %s", responseCode, errorResponse);
        throw new RuntimeException("HTTP request failed: " + responseCode + " - " + errorResponse);
      }
    } finally {
      connection.disconnect();
    }
  }

  private void logInfo(final String message, final Object... args) {
    LogManager.instance().log(this, Level.INFO, message, null, args);
  }

  private void logError(final String message, final Object... args) {
    LogManager.instance().log(this, Level.SEVERE, message, null, args);
  }
}
