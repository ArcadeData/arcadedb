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

import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9386: {@code GET /prom/api/v1/series} must apply the label matchers of {@code match[]}, so it answers the
 * same series {@code /query} answers for the same selector.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9386PromQLSeriesMatchersIT extends BaseGraphServerTest {

  @Test
  void seriesEndpointAppliesTheLabelMatchers() throws Exception {
    testEachServer(serverIndex -> {
      final Database db = getServerDatabase(serverIndex, getDatabaseName());
      db.command("sql", "CREATE TIMESERIES TYPE m9386 TIMESTAMP ts TAGS (host STRING, zone STRING) FIELDS (value DOUBLE)");
      db.transaction(() -> {
        db.command("sql", "INSERT INTO m9386 SET ts = 1000, host = 'h1', zone = 'z1', value = 1.0");
        db.command("sql", "INSERT INTO m9386 SET ts = 1000, host = 'h2', zone = 'z1', value = 2.0");
        db.command("sql", "INSERT INTO m9386 SET ts = 1000, host = 'h3', zone = 'z2', value = 3.0");
      });

      assertThat(hosts(serverIndex, "m9386")).containsExactlyInAnyOrder("h1", "h2", "h3");
      assertThat(hosts(serverIndex, "m9386{host=\"h2\"}")).containsExactly("h2");
      assertThat(hosts(serverIndex, "m9386{host!=\"h2\"}")).containsExactlyInAnyOrder("h1", "h3");
      assertThat(hosts(serverIndex, "m9386{host=~\"h[12]\"}")).containsExactlyInAnyOrder("h1", "h2");
      assertThat(hosts(serverIndex, "m9386{host!~\"h[12]\"}")).containsExactly("h3");
      assertThat(hosts(serverIndex, "m9386{zone=\"z2\"}")).containsExactly("h3");
      assertThat(hosts(serverIndex, "m9386{zone=\"z1\",host=\"h1\"}")).containsExactly("h1");
      assertThat(hosts(serverIndex, "m9386{host=\"nobody\"}")).isEmpty();
      // a label the type does not declare is absent from every series
      assertThat(hosts(serverIndex, "m9386{nolabel=\"x\"}")).isEmpty();
      assertThat(hosts(serverIndex, "m9386{nolabel=\"\"}")).containsExactlyInAnyOrder("h1", "h2", "h3");
      assertThat(hosts(serverIndex, "m9386{nolabel!=\"x\"}")).containsExactlyInAnyOrder("h1", "h2", "h3");
    });
  }

  private List<String> hosts(final int serverIndex, final String selector) throws Exception {
    final String url = getServerHttpUrl(serverIndex,
        "/api/v1/ts/" + getDatabaseName() + "/prom/api/v1/series?match[]=" + URLEncoder.encode(selector, StandardCharsets.UTF_8));
    final HttpURLConnection connection = (HttpURLConnection) new URI(url).toURL().openConnection();
    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    assertThat(connection.getResponseCode()).isEqualTo(200);
    final JSONObject response;
    try (final InputStream is = connection.getInputStream()) {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      is.transferTo(baos);
      response = new JSONObject(baos.toString(StandardCharsets.UTF_8));
    }
    final JSONArray data = response.getJSONArray("data");
    final List<String> hosts = new ArrayList<>();
    for (int i = 0; i < data.length(); i++)
      hosts.add(data.getJSONObject(i).getString("host"));
    return hosts;
  }
}
