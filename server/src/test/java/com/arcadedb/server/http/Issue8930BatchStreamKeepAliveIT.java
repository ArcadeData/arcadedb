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
import com.arcadedb.event.BeforeRecordUpdateListener;
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
 * Regression test for issue #8930: the streamed answer of {@code POST /api/v1/batch} is silent between the last
 * {@code progress} line and the {@code summary} while {@code GraphBatch.close()} finalizes the load, which the driver's
 * silence bound cannot tell from a dead server. The batch stream now carries the same keep-alive newline as the query
 * stream, which {@code readStreamedBatch} already skips.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8930BatchStreamKeepAliveIT extends BaseGraphServerTest {
  private static final long GAP_MS = 1500;

  private volatile boolean armed;

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    db.getSchema().createVertexType("V8930");
    db.getSchema().createEdgeType("E8930");
  }

  @Test
  void theSilentFinalizationOfABatchIsCoveredByKeepAliveNewlines() throws Exception {
    // GraphBatch.close() rewrites the head chunk pointers of the vertices after the last progress line: stall exactly
    // there, the way connecting the deferred incoming edges does on a large load
    getServer(0).getDatabase(getDatabaseName()).getSchema().getType("V8930").getEvents()
        .registerListener((BeforeRecordUpdateListener) record -> {
          if (armed)
            try {
              Thread.sleep(GAP_MS);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          return true;
        });
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_STREAMING_KEEPALIVE_INTERVAL, 200);
    final List<String> lines;
    armed = true;
    try {
      lines = postBatch();
    } finally {
      armed = false;
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_HTTP_STREAMING_KEEPALIVE_INTERVAL,
          GlobalConfiguration.SERVER_HTTP_STREAMING_KEEPALIVE_INTERVAL.getDefValue());
    }

    final List<String> events = lines.stream().filter(l -> !l.isEmpty()).toList();
    assertThat(new JSONObject(events.get(events.size() - 1)).has("summary")).as(lines.toString()).isTrue();
    assertThat(lines.stream().filter(String::isEmpty).count()).as("keep-alive lines during a %d ms gap", GAP_MS)
        .isGreaterThanOrEqualTo(2);
  }

  private List<String> postBatch() throws Exception {
    final HttpURLConnection conn = (HttpURLConnection) new URL(getServerHttpUrl(0, "/api/v1/batch/" + getDatabaseName())).openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
      conn.setRequestProperty("Accept", "application/x-ndjson");
      conn.setRequestProperty("Content-Type", "application/x-ndjson");
      conn.setDoOutput(true);
      final StringBuilder payload = new StringBuilder();
      for (int i = 0; i < 3; i++)
        payload.append("{\"@type\":\"vertex\",\"@class\":\"V8930\",\"@id\":\"v").append(i).append("\",\"id\":").append(i).append("}\n");
      payload.append("{\"@type\":\"edge\",\"@class\":\"E8930\",\"@from\":\"v0\",\"@to\":\"v1\"}\n");
      conn.getOutputStream().write(payload.toString().getBytes(StandardCharsets.UTF_8));
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
