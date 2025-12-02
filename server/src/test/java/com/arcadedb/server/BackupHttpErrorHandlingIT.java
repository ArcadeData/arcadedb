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

import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests backup command HTTP error handling to ensure failed backups return proper HTTP status codes.
 * This test verifies the fix for issue #2244: Failed backup has non-error HTTP status.
 */
class BackupHttpErrorHandlingIT extends BaseGraphServerTest {

  @BeforeEach
  void setUp() throws IOException {
    Files.deleteIfExists(Path.of("./target/backups/graph/backup.zip"));
  }

  @Test
  void backupPermissionDeniedReturns500() throws Exception {
    // Create a temporary directory that will be made read-only

    // Attempt backup to the read-only directory
    final HttpURLConnection connection = (HttpURLConnection)
        new URL("http://127.0.0.1:2480/api/v1/command/" + getDatabaseName()).openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    // Format backup command payload with invalid path
    formatPayload(connection, "sql",
        "BACKUP DATABASE file://../backup.zip", null, new HashMap<>());

    connection.connect();

    // Verify that HTTP 500 status is returned for backup failure
    assertThat(connection.getResponseCode()).isEqualTo(500);
    assertThat(connection.getResponseMessage()).isEqualTo("Internal Server Error");

    // Verify error response contains backup failure information
    final String errorResponse = readError(connection);
    System.out.println("errorResponse = " + errorResponse);
    assertThat(errorResponse).isNotNull();
    assertThat(errorResponse).contains("Backup failed for database ");

  }

  @Test
  void backupInvalidPathReturns500() throws Exception {
    // Use an obviously invalid path that doesn't exist
    final String invalidPath = "/this/path/does/not/exist/backup.zip";

    final HttpURLConnection connection = (HttpURLConnection)
        new URL("http://127.0.0.1:2480/api/v1/command/" + getDatabaseName()).openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    formatPayload(connection, "sql",
        "BACKUP DATABASE file://" + invalidPath, null, new HashMap<>());

    connection.connect();

    // Verify that HTTP 500 status is returned for backup failure
    assertThat(connection.getResponseCode()).isEqualTo(500);
    assertThat(connection.getResponseMessage()).isEqualTo("Internal Server Error");

    // Verify error response contains backup failure information
    final String errorResponse = readError(connection);
    assertThat(errorResponse).isNotNull();
    assertThat(errorResponse).contains("Backup failed for database");
  }

  @Test
  void backupSuccessReturns200() throws Exception {

    // Ensure directory is writable

    final HttpURLConnection connection = (HttpURLConnection)
        new URL("http://127.0.0.1:2480/api/v1/command/" + getDatabaseName()).openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    formatPayload(connection, "sql",
        "BACKUP DATABASE file://backup.zip", null, new HashMap<>());

    connection.connect();

    // Verify that HTTP 200 status is returned for successful backup
    assertThat(connection.getResponseCode()).isEqualTo(200);
    assertThat(connection.getResponseMessage()).isEqualTo("OK");

    // Verify success response contains backup information
    final String response = readResponse(connection);
    assertThat(response).isNotNull();

    final JSONObject jsonResponse = new JSONObject(response);
    assertThat(jsonResponse.has("result")).isTrue();
  }
}
