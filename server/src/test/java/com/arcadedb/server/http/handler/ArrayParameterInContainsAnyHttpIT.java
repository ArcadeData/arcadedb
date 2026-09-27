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
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8436: over {@code POST /api/v1/command}, an array bound to {@code IN} and to {@code CONTAINSANY} answers the
 * same whether it is sent as a positional ({@code "params": [["u1","u2"]]}) or as a named parameter.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ArrayParameterInContainsAnyHttpIT extends BaseGraphServerTest {

  @Test
  void positionalAndNamedArraysAnswerAlike() throws Exception {
    post("CREATE DOCUMENT TYPE T8436", null);
    post("INSERT INTO T8436 SET uuid = 'u1'", null);
    post("INSERT INTO T8436 SET uuid = 'u2'", null);
    post("INSERT INTO T8436 SET uuid = 'u3'", null);

    final JSONArray positional = new JSONArray().put(new JSONArray().put("u1").put("u2"));
    final JSONObject named = new JSONObject().put("ids", new JSONArray().put("u1").put("u2"));

    assertThat(post("SELECT uuid FROM T8436 WHERE uuid IN ?", positional)).containsExactly("u1", "u2");
    assertThat(post("SELECT uuid FROM T8436 WHERE uuid IN (?)", positional)).containsExactly("u1", "u2");
    assertThat(post("SELECT uuid FROM T8436 WHERE uuid IN :ids", named)).containsExactly("u1", "u2");
    assertThat(post("SELECT uuid FROM T8436 WHERE uuid CONTAINSANY ?", positional)).containsExactly("u1", "u2");
    assertThat(post("SELECT uuid FROM T8436 WHERE uuid CONTAINSANY :ids", named)).containsExactly("u1", "u2");
  }

  private List<String> post(final String command, final Object params) throws Exception {
    final JSONObject payload = new JSONObject();
    payload.put("language", "sql");
    payload.put("command", command);
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
    try (final DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
      wr.write(data);
    }
    try {
      assertThat(connection.getResponseCode()).as(command).isEqualTo(200);
      final JSONArray records = new JSONObject(readResponse(connection)).getJSONArray("result");
      final List<String> uuids = new ArrayList<>();
      for (int i = 0; i < records.length(); i++)
        uuids.add(records.getJSONObject(i).getString("uuid", null));
      Collections.sort(uuids, (a, b) -> a == null ? -1 : b == null ? 1 : a.compareTo(b));
      return uuids;
    } finally {
      connection.disconnect();
    }
  }
}
