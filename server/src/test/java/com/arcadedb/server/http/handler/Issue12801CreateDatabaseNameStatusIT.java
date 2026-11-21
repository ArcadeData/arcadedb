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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12801: {@code create database} answered HTTP 500 for a name the caller got wrong when the name held a {@code *} (the engine
 * refuses it after the server's own check let it through) or was longer than a file name can be (the directory cannot be created).
 * Every other refused name answers 400, and so must these.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12801CreateDatabaseNameStatusIT extends BaseGraphServerTest {
  private final HttpClient client = HttpClient.newHttpClient();

  @ParameterizedTest
  @ValueSource(strings = { "star*name", "*", "trailing*" })
  void aNameWithAStarIsRefusedAsABadRequest(final String name) throws Exception {
    final HttpResponse<String> response = executeServerCommand("create database " + name);

    assertThat(response.statusCode()).as("body=%s", response.body()).isEqualTo(400);
    assertThat(executeServerCommand("list databases").body()).doesNotContain(name);
  }

  @Test
  void aNameLongerThanAFileNameIsRefusedAsABadRequest() throws Exception {
    final String name = "x".repeat(256);

    final HttpResponse<String> response = executeServerCommand("create database " + name);

    assertThat(response.statusCode()).as("body=%s", response.body()).isEqualTo(400);
    assertThat(response.body()).as("the reply does not echo the whole path").doesNotContain(name);
  }

  @Test
  void aNameOfMultiByteCharactersPastTheLimitInBytesIsRefusedAsABadRequest() throws Exception {
    // 100 characters, 300 bytes in UTF-8
    final String name = "\u65e5".repeat(100);

    final HttpResponse<String> response = executeServerCommand("create database " + name);

    assertThat(response.statusCode()).as("body=%s", response.body()).isEqualTo(400);
  }

  @Test
  void aNameOfTheLongestAllowedLengthStillWorks() throws Exception {
    final String name = "y".repeat(255);
    try {
      assertThat(executeServerCommand("create database " + name).statusCode()).isEqualTo(200);
    } finally {
      executeServerCommand("drop database " + name);
    }
  }

  private HttpResponse<String> executeServerCommand(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(getServerHttpUrl("/api/v1/server")))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    return client.send(request, BodyHandlers.ofString());
  }
}
