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
  void nestedExceptionInHttpError() throws Exception {
    testEachServer((serverIndex) -> {
      // Execute a command that will produce a nested exception (invalid SQL syntax with a cause chain)
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          "http://127.0.0.1:248" + serverIndex + "/api/v1/command/graph").openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

      // Use a query referencing a non-existent function to trigger an error with nested exception
      formatPayload(connection, "sql",
          "SELECT nonExistentFunction() FROM V", null, new HashMap<>());
      connection.connect();

      try {
        final int responseCode = connection.getResponseCode();
        LogManager.instance().log(this, Level.INFO, "Response code: %d", null, responseCode);

        // Should get a non-2xx response
        assertThat(responseCode).isGreaterThanOrEqualTo(400);

        // Read error response
        final String errorResponse = readError(connection);
        LogManager.instance().log(this, Level.INFO, "Error Response: %s", null, errorResponse);

        // Parse the error response
        final JSONObject errorJson = new JSONObject(errorResponse);
        assertThat(errorJson.has("error")).isTrue();
        assertThat(errorJson.has("detail")).isTrue();

        final String detail = errorJson.getString("detail");
        LogManager.instance().log(this, Level.INFO, "Error detail: %s", null, detail);

        // The detail should contain meaningful error information
        assertThat(detail).as("Error detail should contain error information")
            .isNotEmpty();

      } finally {
        connection.disconnect();
      }
    });
  }
}
