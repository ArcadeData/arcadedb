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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6303, item 3, over the transport it was reported on: {@code POST /command} with {@code awaitResponse=false}.
 * <p>
 * That is a documented way to fire a long DDL statement, and index creation is exactly the kind of long DDL somebody
 * would fire that way. It used to HANG (the command ran on an async worker and the barrier it needs enqueues a marker
 * on every worker including that one); #6281 turned the hang into a clear refusal plus a "run it synchronously"
 * workaround. {@link Issue2097AsyncRebuildIndexIT} pins what survived the refusal - the indexes were at least not
 * destroyed - and this pins what the refusal did not give back: the index is actually built, and it covers every
 * record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6303AsyncDispatchedDDLIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "Issue6303AsyncDispatchedDDL";
  private static final String TYPE_NAME     = "AsyncDDLDoc";
  private static final int    TOT           = 100;

  @Override
  protected String getDatabaseName() {
    return DATABASE_NAME;
  }

  @Override
  protected void populateDatabase() {
    // Each test sets up its own data.
  }

  @Test
  void createIndexWithAwaitResponseFalseBuildsAnIndexThatCoversEveryRecord() throws Exception {
    testEachServer(serverIndex -> {
      final Database database = getServer(serverIndex).getDatabase(getDatabaseName());
      final String typeName = TYPE_NAME + serverIndex;

      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE " + typeName).close();
        database.command("sql", "CREATE PROPERTY " + typeName + ".num LONG").close();
      });
      database.transaction(() -> {
        for (int i = 0; i < TOT; i++)
          database.command("sql", "INSERT INTO " + typeName + " SET num = " + i).close();
      });

      assertThat(post(serverIndex, "CREATE INDEX ON " + typeName + " (num) UNIQUE;")).isEqualTo(202);

      // The command is not on a worker queue any more, but waitCompletion still covers it - that is the property
      // moving the dispatch would otherwise have taken away.
      assertThat(database.async().waitCompletion(60_000)).as("the dispatched command must finish, not park").isTrue();

      assertThat(database.getSchema().getIndexByName(typeName + "[num]").countEntries()).as(
          "an index created with awaitResponse=false must be built, not refused, and must cover every record")
          .isEqualTo(TOT);

      database.transaction(() -> assertThat(
          database.query("sql", "SELECT FROM " + typeName + " WHERE num = 42").stream().count()).isEqualTo(1));
    });
  }

  /** Sends the command with {@code awaitResponse=false} and returns the HTTP status. */
  private int post(final int serverIndex, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(
        "http://localhost:248" + serverIndex + "/api/v1/command/" + DATABASE_NAME).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder()
        .encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    formatPayload(connection,
        new JSONObject().put("language", "sqlscript").put("command", command).put("awaitResponse", false));
    connection.connect();
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }
}
