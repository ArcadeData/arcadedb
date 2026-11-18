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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.handler.AbstractServerHttpHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11739: on an HA cluster the leader answers {@code create database} and {@code drop database} with the
 * {@code X-ArcadeDB-Commit-Index} bookmark, and a follower that forwarded the command answers only once it has applied that
 * index (see {@code RaftCreateDatabaseOnFollowerIT}). A server without HA has no Raft index, so it must answer them as it always
 * did: no bookmark, and no wait.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue11739ServerCommandBookmarkTest extends BaseGraphServerTest {
  private static final String DATABASE = "bookmark11739";

  private final HttpClient http = HttpClient.newHttpClient();

  @AfterEach
  void dropTheDatabase() {
    final ArcadeDBServer server = getServer(0);
    if (server != null && server.isStarted() && server.isDatabaseOfServer(DATABASE))
      new ServerControlPlane(server).dropDatabase(DATABASE);
    http.close();
  }

  @Test
  void aServerWithoutHaAnswersCreateAndDropDatabaseWithoutABookmark() throws Exception {
    assertThat(getServer(0).getHA()).as("this fixture runs without HA").isNull();

    final HttpResponse<String> created = serverCommand("create database " + DATABASE);
    assertThat(created.statusCode()).as(created.body()).isEqualTo(200);
    assertThat(created.headers().firstValue(AbstractServerHttpHandler.COMMIT_INDEX_HEADER_NAME)).isEmpty();

    final HttpResponse<String> dropped = serverCommand("drop database " + DATABASE);
    assertThat(dropped.statusCode()).as(dropped.body()).isEqualTo(200);
    assertThat(dropped.headers().firstValue(AbstractServerHttpHandler.COMMIT_INDEX_HEADER_NAME)).isEmpty();
  }

  private HttpResponse<String> serverCommand(final String command) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(getServerHttpUrl("/api/v1/server")))
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .setHeader("Authorization", "Basic " + Base64.getEncoder()
            .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .build();
    return http.send(request, HttpResponse.BodyHandlers.ofString());
  }
}
