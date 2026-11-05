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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.MaintenanceCoordinator;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11310: the control plane asked whether a database exists with the question of the data path, "may a client's first
 * request open it", which is false for a database an operator holds - closed by {@code close database}, reserved by a restore,
 * being dropped. Such a database exists: its name is refused by a create, and an operator acts on it. So {@code GET /exists}
 * answered false for a database a create of the same name then refused with 409, and {@code drop database} answered "does not
 * exist" for a database on disk that {@code open database} brought straight back. The invariant pinned here, for each state: the
 * name exists exactly when a create refuses it, and a drop either drops it or refuses with the reason it is held.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue11310HeldDatabaseExistsTest extends BaseGraphServerTest {
  private static final String HELD = "held11310";

  private final HttpClient http = HttpClient.newHttpClient();

  @AfterEach
  void dropTheHeldDatabase() {
    final ArcadeDBServer server = getServer(0);
    if (server == null || !server.isStarted())
      return;
    if (server.isDatabaseOfServer(HELD))
      new ServerControlPlane(server).dropDatabase(HELD);
  }

  @Test
  void anOpenDatabaseExistsAndItsNameIsTaken() throws Exception {
    assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(200);

    assertThat(exists(HELD)).isTrue();
    assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(409);
  }

  @Test
  void aDatabaseClosedByCommandExistsAndIsDropped() throws Exception {
    assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(200);
    assertThat(serverCommand("close database " + HELD).statusCode()).isEqualTo(200);

    // IT USED TO ANSWER false, AND THE CREATE THAT FOLLOWED 409
    assertThat(exists(HELD)).isTrue();
    assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(409);

    // IT USED TO ANSWER "does not exist" AND LEAVE THE DATABASE ON DISK
    final HttpResponse<String> drop = serverCommand("drop database " + HELD);
    assertThat(drop.statusCode()).as(drop.body()).isEqualTo(200);
    assertThat(databaseDirectory(HELD)).doesNotExist();
    assertThat(exists(HELD)).isFalse();
    assertThat(getServer(0).isDatabaseHeld(HELD)).as("the close mark goes with the database").isFalse();
  }

  @Test
  void aDatabaseBeingRestoredExistsAndIsNotDroppedFromUnderTheRestore() throws Exception {
    assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(200);
    beginRestore();
    try {
      assertThat(exists(HELD)).isTrue();
      assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(409);

      // REFUSED WITH THE REASON IT IS HELD, NEVER "does not exist"
      final HttpResponse<String> drop = serverCommand("drop database " + HELD);
      assertThat(drop.statusCode()).isEqualTo(409);
      assertThat(drop.body()).contains("restore").doesNotContain("does not exist");
      assertThat(databaseDirectory(HELD)).exists();
    } finally {
      endRestore();
    }

    assertThat(serverCommand("drop database " + HELD).statusCode()).isEqualTo(200);
    assertThat(exists(HELD)).isFalse();
  }

  @Test
  void aNameReservedForTheRestoreOfANewDatabaseExistsWhileTheRestoreRuns() throws Exception {
    beginRestore();
    try {
      assertThat(exists(HELD)).isTrue();
      assertThat(serverCommand("create database " + HELD).statusCode()).isEqualTo(409);
    } finally {
      endRestore();
    }
    assertThat(exists(HELD)).isFalse();
  }

  /** What a restore takes, in its order: the maintenance slot of the database, then the claim on its name. */
  private void beginRestore() {
    assertThat(getServer(0).getBackupCoordinator().begin(HELD, MaintenanceCoordinator.Operation.RESTORE)).isNull();
    getServer(0).reserveDatabaseNameForRestore(HELD);
  }

  private void endRestore() {
    getServer(0).releaseDatabaseNameReservedForRestore(HELD);
    getServer(0).getBackupCoordinator().end(HELD, MaintenanceCoordinator.Operation.RESTORE);
  }

  @Test
  void aNameThatIsNoDatabaseDoesNotExist() throws Exception {
    assertThat(exists(HELD)).isFalse();
    final HttpResponse<String> drop = serverCommand("drop database " + HELD);
    assertThat(drop.statusCode()).isEqualTo(400);
  }

  private File databaseDirectory(final String name) {
    return new File(getServer(0).getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), name);
  }

  private boolean exists(final String name) throws Exception {
    final HttpResponse<String> response = http.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/exists/" + name)))
        .header("Authorization", authorization()).GET().build(), HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
    return new JSONObject(response.body()).getBoolean("result");
  }

  private HttpResponse<String> serverCommand(final String command) throws Exception {
    return http.send(HttpRequest.newBuilder(URI.create(getServerHttpUrl(0, "/api/v1/server")))
        .header("Authorization", authorization()).header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(new JSONObject().put("command", command).toString(), StandardCharsets.UTF_8))
        .build(), HttpResponse.BodyHandlers.ofString());
  }

  private static String authorization() {
    return "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8));
  }
}
