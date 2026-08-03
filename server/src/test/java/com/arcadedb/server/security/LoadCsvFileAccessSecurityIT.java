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
package com.arcadedb.server.security;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GHSA-hfp5-6gcp-8c75: the OpenCypher {@code LOAD CSV FROM 'file:///...'} clause read arbitrary
 * files from the ArcadeDB host and streamed each line back as a result row, for any authenticated user who could run a
 * read query.
 *
 * <p>The {@code file://} branch of {@code LoadCSVStep.openRawInputStream} had no authorization check at all - the only
 * {@code checkPermissionsOnDatabase} in the whole OpenCypher engine gates user-management statements. The two settings
 * that could have restricted it are open by default ({@code arcadedb.opencypher.loadCsv.allowFileUrls=true},
 * {@code arcadedb.opencypher.loadCsv.importDirectory=""}), and although production mode force-disables file access at
 * startup, {@code arcadedb.server.mode} defaults to {@code development} and nothing in the distribution or the Docker
 * image sets it, so a stock server was exposed.</p>
 *
 * <p>The fix gates the local-file branch on {@code updateSecurity}, the same privilege the other local-file-reading
 * statement ({@code IMPORT DATABASE}) requires - see {@link ImportDatabaseSecurityIT}. This is a no-op in embedded
 * mode, so embedded applications that load local CSV files are unaffected.</p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LoadCsvFileAccessSecurityIT extends BaseGraphServerTest {

  private static final String SECRET = "hfp5-sentinel-secret-value";

  @Test
  void nonAdminUserCannotReadLocalFileViaLoadCsv() throws Exception {
    testEachServer(serverIndex -> {
      final File secretFile = writeSecretFile();
      final String userAuth = createNonAdminUser(serverIndex, "csvpwn", "pwn123secret");
      try {
        // The published proof-of-concept, pointed at a sentinel file instead of /etc/passwd so the assertion can prove
        // the contents did NOT come back rather than merely that the request failed.
        final Response response = command(serverIndex, userAuth, "cypher",
            "LOAD CSV FROM 'file:///" + secretFile.getAbsolutePath().replace("\\", "/") + "' AS row RETURN row");

        assertThat(response.status).as("non-admin user must not read local files via LOAD CSV").isEqualTo(403);
        assertThat(response.body).as("the file contents must not leak in the response body").doesNotContain(SECRET);
      } finally {
        dropUser(serverIndex, "csvpwn");
        secretFile.delete();
      }
    });
  }

  @Test
  void nonAdminUserCannotReadLocalFileViaBarePath() throws Exception {
    testEachServer(serverIndex -> {
      final File secretFile = writeSecretFile();
      final String userAuth = createNonAdminUser(serverIndex, "csvpwn2", "pwn123secret");
      try {
        // A bare filesystem path (no file:// scheme) reaches the same branch of openRawInputStream, so the gate has to
        // cover it too - otherwise dropping the scheme would sidestep the fix.
        final Response response = command(serverIndex, userAuth, "cypher",
            "LOAD CSV FROM '" + secretFile.getAbsolutePath().replace("\\", "/") + "' AS row RETURN row");

        assertThat(response.status).as("non-admin user must not read local files via a bare LOAD CSV path").isEqualTo(403);
        assertThat(response.body).as("the file contents must not leak in the response body").doesNotContain(SECRET);
      } finally {
        dropUser(serverIndex, "csvpwn2");
        secretFile.delete();
      }
    });
  }

  @Test
  void adminUserCanStillLoadLocalCsv() throws Exception {
    testEachServer(serverIndex -> {
      final File secretFile = writeSecretFile();
      try {
        // Positive control: LOAD CSV from a local file remains available to an administrative user, exactly like
        // IMPORT DATABASE. A fix that simply disabled the feature would pass the two tests above but break this one.
        final Response response = command(serverIndex, basicAuth(), "cypher",
            "LOAD CSV WITH HEADERS FROM 'file:///" + secretFile.getAbsolutePath().replace("\\", "/")
                + "' AS row RETURN row.secret AS secret");

        assertThat(response.status).as("admin must still be able to load a local CSV").isEqualTo(200);
        assertThat(response.body).as("admin gets the file contents").contains(SECRET);
      } finally {
        secretFile.delete();
      }
    });
  }

  private File writeSecretFile() throws IOException {
    final File file = File.createTempFile("hfp5-loadcsv", ".csv");
    Files.writeString(file.toPath(), "secret\n" + SECRET + "\n", StandardCharsets.UTF_8);
    return file;
  }

  /**
   * Creates a user that is a member of the database but only of the default (non-admin) group, so it has none of the
   * {@code updateSecurity / updateSchema / updateDatabaseSettings} permissions.
   */
  private String createNonAdminUser(final int serverIndex, final String name, final String password) throws Exception {
    if (getServer(serverIndex).getSecurity().existsUser(name))
      getServer(serverIndex).getSecurity().dropUser(name);

    final JSONObject payload = new JSONObject();
    payload.put("name", name);
    payload.put("password", password);
    // "reader" is not a defined group, so the user falls back to the default group which has empty access
    payload.put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("reader")));

    final HttpURLConnection connection = open(serverIndex, "/api/v1/server/users", "POST", basicAuth());
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
    return "Basic " + Base64.getEncoder().encodeToString((name + ":" + password).getBytes());
  }

  private void dropUser(final int serverIndex, final String name) {
    try {
      if (getServer(serverIndex) != null && getServer(serverIndex).getSecurity().existsUser(name))
        getServer(serverIndex).getSecurity().dropUser(name);
    } catch (final Exception ignore) {
    }
  }

  private record Response(int status, String body) {
  }

  private Response command(final int serverIndex, final String auth, final String language, final String query)
      throws Exception {
    final HttpURLConnection connection = open(serverIndex, "/api/v1/command/" + getDatabaseName(), "POST", auth);
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/json");
    final JSONObject payload = new JSONObject().put("language", language).put("command", query);
    connection.getOutputStream().write(payload.toString().getBytes());
    connection.connect();
    try {
      final int status = connection.getResponseCode();
      final var stream = status < 400 ? connection.getInputStream() : connection.getErrorStream();
      final String body = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body);
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection open(final int serverIndex, final String path, final String method, final String auth)
      throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL("http://127.0.0.1:248" + serverIndex + path).openConnection();
    connection.setRequestMethod(method);
    connection.setRequestProperty("Authorization", auth);
    return connection;
  }

  private String basicAuth() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes());
  }
}
