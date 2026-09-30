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
package com.arcadedb.server.http;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8565: a streamed query whose next row takes a while to produce was silent on the wire,
 * which a client bounding silence cannot tell from a dead server. The server now writes a bare newline, which every
 * consumer of the encoding skips, whenever nothing has been flushed for {@code arcadedb.server.httpStreamingKeepAliveInterval}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8565StreamKeepAliveIT extends BaseGraphServerTest {
  private static final String TYPE_NAME = "KeepAlive8565";
  private static final long   GAP_MS    = 1500;

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE_NAME + " BUCKETS 1");
      db.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".idx INTEGER");
      for (int i = 0; i < 3; i++)
        db.newDocument(TYPE_NAME).set("idx", i).save();
    });
    ((SQLQueryEngine) db.getQueryEngine("sql")).getFunctionFactory().register(new SQLFunctionAbstract("slowRow8565") {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        final int idx = ((Number) params[0]).intValue();
        if (idx == 1)
          try {
            Thread.sleep(GAP_MS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        return idx;
      }

      @Override
      public String getSyntax() {
        return "slowRow8565(<n>)";
      }
    });
  }

  @Test
  void aSlowRowIsPrecededByKeepAliveNewlines() throws Exception {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_STREAMING_KEEPALIVE_INTERVAL, 200);

    final List<String> lines = streamQuery();

    assertThat(lines.stream().filter(String::isEmpty).count()).as("keep-alive lines during a %d ms gap", GAP_MS).isGreaterThanOrEqualTo(2);
    final List<String> events = lines.stream().filter(l -> !l.isEmpty()).toList();
    assertThat(events).hasSize(4);
    for (int i = 0; i < 3; i++)
      assertThat(new JSONObject(events.get(i)).getJSONObject("record").getInt("v")).isEqualTo(i);
    assertThat(new JSONObject(events.get(3)).has("stats")).isTrue();
  }

  @Test
  void zeroDisablesTheKeepAlive() throws Exception {
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_STREAMING_KEEPALIVE_INTERVAL, 0);

    final List<String> lines = streamQuery();

    assertThat(lines).noneMatch(String::isEmpty);
    assertThat(lines).hasSize(4);
  }

  private List<String> streamQuery() throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URL(getServerHttpUrl(0, "/api/v1/query/" + getDatabaseName())).openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setRequestProperty("Accept", "application/x-ndjson");
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);
      conn.getOutputStream().write(new JSONObject().put("language", "sql")
          .put("command", "SELECT slowRow8565(idx) AS v FROM " + TYPE_NAME).toString().getBytes(StandardCharsets.UTF_8));
      assertThat(conn.getResponseCode()).isEqualTo(200);

      final List<String> lines = new ArrayList<>();
      try (final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null)
          lines.add(line);
      }
      return lines;
    } finally {
      conn.disconnect();
    }
  }
}
