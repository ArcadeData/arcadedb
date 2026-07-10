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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class QueryMetricsProtocolIT extends BaseGraphServerTest {
  @Override
  protected int getServerCount() {
    return 1;
  }

  @Test
  void httpQueryRecordedWithProtocolTag() throws Exception {
    final HttpURLConnection c = (HttpURLConnection) URI.create("http://localhost:2480/api/v1/query/graph").toURL().openConnection();
    c.setRequestMethod("POST");
    c.setDoOutput(true);
    c.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    c.setRequestProperty("Content-Type", "application/json");
    c.getOutputStream().write("{\"language\":\"sql\",\"command\":\"select 1 as one\"}".getBytes(StandardCharsets.UTF_8));
    c.connect();
    assertThat(c.getResponseCode()).isEqualTo(200);
    c.disconnect();

    final Timer timer = Metrics.globalRegistry.find("arcadedb.query.duration")
        .tag("protocol", "http").tag("db", "graph").tag("language", "sql").tag("type", "query").timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isGreaterThanOrEqualTo(1L);
  }

  @Test
  void protocolTagIsBoundedAndQueryTextIsNeverATag() throws Exception {
    httpQueryRecordedWithProtocolTag(); // populate at least one series

    final var timers = Metrics.globalRegistry.find("arcadedb.query.duration").timers();
    assertThat(timers).isNotEmpty();
    for (final Timer t : timers) {
      final String protocol = t.getId().getTag("protocol");
      assertThat(protocol).isIn("http", "bolt", "postgres", "mongo", "grpc", "redis", "internal");
      t.getId().getTags().forEach(tag ->
          assertThat(tag.getValue().toLowerCase()).doesNotContain("select"));
    }
  }
}
