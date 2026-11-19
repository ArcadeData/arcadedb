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
package com.arcadedb.server.security;

import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11797: {@code CHECK DATABASE FIX}, with any of its repair clauses, and {@code CHECK DATABASE
 * COMPRESS} ran for a user of the built-in {@code readonly} and {@code crud} groups, while {@code REBUILD INDEX} and
 * {@code COMPACT INDEX} answered 403. A check that repairs deletes and relinks records, rebuilds indexes, removes files and
 * rewrites pages, through the buckets, so no per-type grant is asked on the way: the statement itself has to ask.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseFixAuthorizationIT extends BaseGraphServerTest {
  private static final String   PASSWORD = "password12345";
  private static final String[] REPAIRS  = { "CHECK DATABASE FIX", "CHECK DATABASE COMPRESS", "CHECK DATABASE FIX COMPRESS",
      "CHECK DATABASE FIX DELETE ORPHANS", "CHECK DATABASE FIX DELETE INVALID RECORDS",
      "CHECK DATABASE FIX RECLAIM UNREFERENCED FILES",
      "CHECK DATABASE FIX DELETE ORPHANS DELETE INVALID RECORDS RECLAIM UNREFERENCED FILES COMPRESS",
      "CHECK DATABASE TYPE Fix11797 FIX", "CHECK DATABASE FIX DEEP" };

  @Test
  void aUserWithoutUpdateSchemaCannotRepair() throws Exception {
    testEachServer(serverIndex -> {
      assertThat(command(serverIndex, rootAuth(), "CREATE VERTEX TYPE Fix11797").status).isEqualTo(200);

      for (final String group : new String[] { "readonly", "crud" }) {
        final String user = "fix11797-" + group;
        final String auth = createUser(serverIndex, user, group);
        try {
          for (final String repair : REPAIRS) {
            final Response response = command(serverIndex, auth, repair);
            assertThat(response.status).as("a user of the '%s' group must not run %s", group, repair).isEqualTo(403);
            assertThat(response.body).as("the refusal of %s names the statement and the permission it takes", repair)
                .contains("CHECK DATABASE").contains("updateSchema").contains(user);
          }

          // A check that repairs nothing is a read, and stays one
          assertThat(command(serverIndex, auth, "CHECK DATABASE").status).as("'%s' can still CHECK DATABASE", group).isEqualTo(200);
          assertThat(command(serverIndex, auth, "CHECK DATABASE DEEP").status).as("'%s' can still CHECK DATABASE DEEP", group)
              .isEqualTo(200);
          assertThat(command(serverIndex, auth, "CHECK DATABASE TYPE Fix11797").status).as("'%s' can still check a type", group)
              .isEqualTo(200);
        } finally {
          dropUser(serverIndex, user);
        }
      }
    });
  }

  /** The refusal has to come before the checker starts: what a refused FIX would have deleted is still there. */
  @Test
  void aRefusedRepairDeletesNothing() throws Exception {
    testEachServer(serverIndex -> {
      // A record that violates its type's existence constraint, which DELETE INVALID RECORDS removes
      assertThat(command(serverIndex, rootAuth(), "CREATE DOCUMENT TYPE Invalid11797").status).isEqualTo(200);
      assertThat(command(serverIndex, rootAuth(), "INSERT INTO Invalid11797 SET other = 1").status).isEqualTo(200);
      // Through the API: a mandatory property created on a populated type does not validate the records it already holds
      getServerDatabase(serverIndex, getDatabaseName()).getSchema().getType("Invalid11797").createProperty("must", Type.STRING)
          .setMandatory(true);
      assertThat(countInvalid(serverIndex)).isEqualTo(1);

      final String auth = createUser(serverIndex, "fix11797-victim", "crud");
      try {
        assertThat(command(serverIndex, auth, "CHECK DATABASE FIX DELETE INVALID RECORDS").status).isEqualTo(403);
        assertThat(countInvalid(serverIndex)).as("the refused repair deleted nothing").isEqualTo(1);
      } finally {
        dropUser(serverIndex, "fix11797-victim");
      }

      // Positive control: the same statement, run by an administrator, does remove it - so the 403 above refused a real delete
      assertThat(command(serverIndex, rootAuth(), "CHECK DATABASE FIX DELETE INVALID RECORDS").status).isEqualTo(200);
      assertThat(countInvalid(serverIndex)).as("the administrator's repair deleted the invalid record").isEqualTo(0);
    });
  }

  @Test
  void aUserWithUpdateSchemaCanStillRepair() throws Exception {
    testEachServer(serverIndex -> {
      for (final String repair : REPAIRS)
        if (!repair.contains("TYPE"))
          assertThat(command(serverIndex, rootAuth(), repair).status).as("root can run %s", repair).isEqualTo(200);

      final String auth = createUser(serverIndex, "fix11797-admin", "admin");
      try {
        assertThat(command(serverIndex, auth, "CHECK DATABASE FIX").status).as("a user of the 'admin' group can FIX").isEqualTo(200);
        assertThat(command(serverIndex, auth, "CHECK DATABASE COMPRESS").status).as("a user of the 'admin' group can COMPRESS")
            .isEqualTo(200);
      } finally {
        dropUser(serverIndex, "fix11797-admin");
      }
    });
  }

  private long countInvalid(final int serverIndex) throws Exception {
    final Response response = command(serverIndex, rootAuth(), "SELECT count(@rid) AS total FROM Invalid11797");
    assertThat(response.status).isEqualTo(200);
    return new JSONObject(response.body).getJSONArray("result").getJSONObject(0).getLong("total");
  }

  private Response command(final int serverIndex, final String auth, final String sql) throws Exception {
    final HttpURLConnection connection = open(serverIndex, "/api/v1/command/" + getDatabaseName(), auth);
    connection.getOutputStream()
        .write(new JSONObject().put("language", "sql").put("command", sql).toString().getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      final int status = connection.getResponseCode();
      try (final InputStream in = status < 400 ? connection.getInputStream() : connection.getErrorStream()) {
        return new Response(status, in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
    } finally {
      connection.disconnect();
    }
  }

  private String createUser(final int serverIndex, final String name, final String group) throws Exception {
    dropUser(serverIndex, name);

    final JSONObject payload = new JSONObject().put("name", name).put("password", PASSWORD)
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(group)));

    final HttpURLConnection connection = open(serverIndex, "/api/v1/server/users", rootAuth());
    connection.getOutputStream().write(payload.toString().getBytes(StandardCharsets.UTF_8));
    connection.connect();
    try {
      assertThat(connection.getResponseCode()).isEqualTo(201);
    } finally {
      connection.disconnect();
    }
    return basic(name, PASSWORD);
  }

  private void dropUser(final int serverIndex, final String name) {
    if (getServer(serverIndex).getSecurity().existsUser(name))
      getServer(serverIndex).getSecurity().dropUser(name);
  }

  private HttpURLConnection open(final int serverIndex, final String path, final String auth) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URL(getServerHttpUrl(serverIndex, path)).openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization", auth);
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    return connection;
  }

  private String rootAuth() {
    return basic("root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  private static String basic(final String user, final String password) {
    return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  private record Response(int status, String body) {
  }
}
