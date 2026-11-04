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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10488: a list nested in a list or in a map HTTP parameter was stored as the primitive
 * {@code long[]}/{@code double[]} the request parser leaves at any depth, so {@code =}, {@code .type()} and {@code GROUP BY}
 * saw a different value than for the same list written as a literal. The properties are NOT declared, which is the common
 * schemaless case and the one a conversion on declared LIST/MAP properties alone would miss.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue10488NestedArrayParamHttpIT extends BaseGraphServerTest {

  @Test
  void aNestedListParameterIsStoredAsAListLikeTheLiteral() throws Exception {
    command("CREATE DOCUMENT TYPE NestedParam", null);

    command("INSERT INTO NestedParam SET k = 'param', m = :m, ll = :ll, md = :md",
        new JSONObject("{\"m\":{\"a\":[1,2]},\"ll\":[[1,2],[3]],\"md\":{\"a\":[1.5,2.5]}}"));
    command("INSERT INTO NestedParam SET k = 'literal', m = {\"a\": [1, 2]}, ll = [[1, 2], [3]], md = {\"a\": [1.5, 2.5]}", null);

    assertThat(keys("SELECT k FROM NestedParam WHERE m.a = [1, 2] ORDER BY k")).containsExactly("literal", "param");
    assertThat(keys("SELECT k FROM NestedParam WHERE ll[0] = [1, 2] ORDER BY k")).containsExactly("literal", "param");
    assertThat(keys("SELECT k FROM NestedParam WHERE md.a = [1.5, 2.5] ORDER BY k")).containsExactly("literal", "param");
    assertThat(keys("SELECT k FROM NestedParam WHERE m.a.type() = 'LIST' ORDER BY k")).containsExactly("literal", "param");

    final JSONArray groups = new JSONObject(post("SELECT count(*) AS c FROM NestedParam GROUP BY m", null)).getJSONArray("result");
    assertThat(groups.length()).as("one group for the two equal maps").isEqualTo(1);
    assertThat(groups.getJSONObject(0).getInt("c")).isEqualTo(2);
  }

  private List<String> keys(final String sql) throws Exception {
    final JSONArray rows = new JSONObject(post(sql, null)).getJSONArray("result");
    final List<String> keys = new ArrayList<>();
    for (int i = 0; i < rows.length(); i++)
      keys.add(rows.getJSONObject(i).getString("k"));
    return keys;
  }

  private void command(final String sql, final JSONObject params) throws Exception {
    post(sql, params);
  }

  private String post(final String sql, final JSONObject params) throws Exception {
    final JSONObject payload = new JSONObject().put("language", "sql").put("command", sql);
    if (params != null)
      payload.put("params", params);

    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl("/api/v1/command/") + getDatabaseName()).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setDoOutput(true);
    final byte[] data = payload.toString().getBytes(StandardCharsets.UTF_8);
    connection.setRequestProperty("Content-Length", Integer.toString(data.length));
    try (final DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
      out.write(data);
    }
    connection.connect();

    try {
      final int status = connection.getResponseCode();
      final String body = status >= 400
          ? (connection.getErrorStream() != null ? new String(connection.getErrorStream().readAllBytes(), StandardCharsets.UTF_8)
              : "<no error stream>")
          : readResponse(connection);
      if (status != 200)
        throw new AssertionError("HTTP " + status + " for command [" + sql + "]: " + body);
      return body;
    } finally {
      connection.disconnect();
    }
  }
}
