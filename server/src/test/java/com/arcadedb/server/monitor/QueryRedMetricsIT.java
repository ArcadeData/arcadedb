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
package com.arcadedb.server.monitor;

import com.arcadedb.server.BaseGraphServerTest;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the always-on query/command RED timer {@code arcadedb.query.duration} records executions
 * tagged by database, language and type, never by the (high-cardinality) query text.
 */
class QueryRedMetricsIT extends BaseGraphServerTest {

  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void queryDurationTimerIsTaggedByDatabaseAndLanguage() throws Exception {
    post("/api/v1/query/graph", "{\"language\":\"sql\",\"command\":\"select 1 as one\"}");

    // protocol is pinned too: the ITs share one reused fork, so a meter left by an earlier test class
    // under the same db/language/type but a different protocol would otherwise match first and be
    // asserted on, failing on a count this test never contributed to.
    final Timer queryTimer = Metrics.globalRegistry.find("arcadedb.query.duration")
        .tag("protocol", "http").tag("db", "graph").tag("language", "sql").tag("type", "query").timer();
    assertThat(queryTimer).isNotNull();
    assertThat(queryTimer.count()).isGreaterThanOrEqualTo(1L);
  }

  @Test
  void commandDurationTimerIsTaggedAsCommand() throws Exception {
    post("/api/v1/command/graph", "{\"language\":\"sql\",\"command\":\"select 1 as one\"}");

    final Timer commandTimer = Metrics.globalRegistry.find("arcadedb.query.duration")
        .tag("protocol", "http").tag("db", "graph").tag("language", "sql").tag("type", "command").timer();
    assertThat(commandTimer).isNotNull();
    assertThat(commandTimer.count()).isGreaterThanOrEqualTo(1L);
  }

  private void post(final String path, final String body) throws Exception {
    final HttpURLConnection c = (HttpURLConnection) new URL("http://localhost:2480" + path).openConnection();
    c.setRequestMethod("POST");
    c.setDoOutput(true);
    c.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    c.setRequestProperty("Content-Type", "application/json");
    c.getOutputStream().write(body.getBytes(StandardCharsets.UTF_8));
    c.connect();
    assertThat(c.getResponseCode()).isEqualTo(200);
    c.disconnect();
  }
}
