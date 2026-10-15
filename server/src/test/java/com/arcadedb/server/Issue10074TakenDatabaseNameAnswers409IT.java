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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static com.arcadedb.engine.ComponentFile.MODE.READ_WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10074: {@code create database <name>} on a name that is taken answered 400 {@code Cannot execute command},
 * the status of a malformed request, while a name reserved for a running restore already answered 409 (issue #7441)
 * and gRPC's {@code CreateDatabase} already answered {@code ALREADY_EXISTS}. A client provisioning databases wants the
 * same create-if-absent signal it gets for users (issue #10073).
 * <p>
 * The server refuses a taken name at two places in {@code ArcadeDBServer.createDatabase}: the registry and the
 * directory on disk. Both are driven, over HTTP, because they are two throw sites that must agree. The tests also pin
 * what is deliberately NOT changed: the type stays an {@link IllegalArgumentException}, so an embedded caller that
 * catches it keeps working, and a malformed name still answers 400.
 */
class Issue10074TakenDatabaseNameAnswers409IT extends BaseGraphServerTest {

  @Test
  void aNameRegisteredOnTheServerAnswers409() throws Exception {
    final String name = getDatabaseName() + "_10074_registered";
    try {
      final HttpResponse<String> created = postCommand("create database " + name);
      assertThat(created.statusCode()).isEqualTo(200);
      assertThat(getServer(0).existsDatabase(name)).isTrue();

      final HttpResponse<String> second = postCommand("create database " + name);
      assertThat(second.statusCode()).isEqualTo(409);
      assertThat(second.body()).contains("already exists");

      // The refusal must not have touched the database that is there
      assertThat(getServer(0).existsDatabase(name)).isTrue();
    } finally {
      dropIfPresent(name);
    }
  }

  @Test
  void theDatabaseTheFixtureSeededAnswers409() throws Exception {
    final HttpResponse<String> refused = postCommand("create database " + getDatabaseName());

    assertThat(refused.statusCode()).isEqualTo(409);
    assertThat(refused.body()).contains("already exists");
  }

  /** The second throw site: a directory is there that the server never registered, as after a half-finished operation. */
  @Test
  void aDirectoryThatIsOnDiskButNotRegisteredAnswers409() throws Exception {
    final String name = getDatabaseName() + "_10074_unregistered";
    final File directory = new File(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY),
        name);
    try {
      new DatabaseFactory(directory.getPath()).create().close();
      assertThat(getServer(0).existsDatabase(name)).isFalse();

      final HttpResponse<String> refused = postCommand("create database " + name);

      assertThat(refused.statusCode()).isEqualTo(409);
      assertThat(refused.body()).contains("already exists");
    } finally {
      FileUtils.deleteRecursively(directory);
    }
  }

  /**
   * {@code import database} creates its database through the same server call, so it gets the same answer. Both forms:
   * the plain one, and the progress stream ({@code Accept: text/event-stream}), which can still answer with a status
   * because the refusal comes before anything was streamed. The URL is never fetched: the name is refused first.
   */
  @Test
  void importDatabaseIntoATakenNameAnswers409InBothForms() throws Exception {
    final String url = "file://" + new File("target/issue10074-never-read.zip").getAbsolutePath();

    final HttpResponse<String> plain = postCommand("import database " + getDatabaseName() + " " + url);
    assertThat(plain.statusCode()).isEqualTo(409);
    assertThat(plain.body()).contains("already exists");

    final HttpResponse<String> streamed = postCommand("import database " + getDatabaseName() + " " + url,
        "text/event-stream");
    assertThat(streamed.statusCode()).isEqualTo(409);
    assertThat(streamed.body()).contains("already exists");
  }

  /** Embedded callers catching the old type keep working: the conflict is a refinement of {@link IllegalArgumentException}. */
  @Test
  void theConflictIsStillAnIllegalArgumentExceptionForEmbeddedCallers() {
    assertThatThrownBy(() -> getServer(0).createDatabase(getDatabaseName(), READ_WRITE))
        .isInstanceOf(DatabaseAlreadyExistsException.class)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Database '" + getDatabaseName() + "' already exists");
  }

  /** The other half of the contract: 400 still means a malformed request. */
  @Test
  void aMalformedNameStillAnswers400() throws Exception {
    final HttpResponse<String> refused = postCommand("create database ../escape");

    assertThat(refused.statusCode()).isEqualTo(400);
    // Refused by the name check, not by anything else that happens to answer 400
    assertThat(refused.body()).contains("Database name '../escape'").doesNotContain("already exists");
  }

  private void dropIfPresent(final String name) throws Exception {
    if (getServer(0).existsDatabase(name))
      getServer(0).getDatabase(name).getEmbedded().drop();
    getServer(0).removeDatabase(name);
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    // The import tests name a local file, which the SSRF guard refuses by default before the name is even looked at
    config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
  }

  private HttpResponse<String> postCommand(final String command) throws Exception {
    return postCommand(command, "application/json");
  }

  private HttpResponse<String> postCommand(final String command, final String accept) throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .header("Accept", accept)
        .uri(URI.create(getServerHttpUrl(0, "/api/v1/server")))
        .header("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString()))
        .build();
    try (final HttpClient client = HttpClient.newHttpClient()) {
      return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }
}
