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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for issue #5037 item 1, verifying the {@code isProductionMode()} wiring end-to-end (not just
 * {@code buildErrorBody} in isolation). With {@code arcadedb.server.mode=production} a typed error over HTTP must:
 * conceal the free-form {@code detail} cause chain (no file paths / engine internals leaked), yet preserve the
 * {@code exception} class name and structured {@code exceptionArgs} on which the remote Java driver and HA
 * leader-exception reconstruction depend to rebuild the typed exception and its duplicate-key details.
 */
class Issue5037ProductionErrorBodyIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_MODE.setValue("production");
  }

  @Test
  void productionModePreservesTypedExceptionContractButConcealsCauseChain() throws Exception {
    // Build a unique index so a second insert with the same key raises a DuplicatedKeyException (409),
    // which carries structured exceptionArgs (index name | keys | existing RID).
    command(0, "CREATE VERTEX TYPE Person5037");
    command(0, "CREATE PROPERTY Person5037.name STRING");
    command(0, "CREATE INDEX ON Person5037 (name) UNIQUE");
    command(0, "INSERT INTO Person5037 SET name = 'duplicate'");

    final HttpURLConnection connection = post("INSERT INTO Person5037 SET name = 'duplicate'");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(409);

      final String body = FileUtils.readStreamAsString(connection.getErrorStream(), "utf8");
      final JSONObject json = new JSONObject(body);

      // Generic, static top-level message - never the raw exception text.
      assertThat(json.getString("error")).isEqualTo("Found duplicate key in index");

      // Wire contract preserved so the remote driver / HA reconstruct the typed exception and its details.
      assertThat(json.getString("exception")).isEqualTo(DuplicatedKeyException.class.getName());
      assertThat(json.has("exceptionArgs")).isTrue();
      assertThat(json.getString("exceptionArgs")).contains("Person5037");

      // Free-form cause chain concealed in production.
      assertThat(json.has("detail")).isFalse();
    } finally {
      connection.disconnect();
    }
  }

  private HttpURLConnection post(final String sqlCommand) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:2480/api/v1/command/graph").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    final byte[] payload = new JSONObject().put("language", "sql").put("command", sqlCommand).toString()
        .getBytes(StandardCharsets.UTF_8);
    connection.setRequestProperty("Content-Length", Integer.toString(payload.length));
    try (final DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
      wr.write(payload);
    }
    connection.connect();
    return connection;
  }
}
