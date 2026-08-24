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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ai.ToolDispatcher;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the AI chat {@code query_database} tool bypassing per-type/bucket ACL enforcement:
 * {@link ToolDispatcher#executeQuery} ran the user-supplied query with only the coarse
 * {@code canAccessToDatabase} check and never bound the authenticated principal onto the query thread's
 * {@code DatabaseContext}. The engine's fine-grained per-type ACL layer
 * ({@code LocalDatabase.checkPermissionsOnFile}) is deliberately a no-op when no principal is bound, so a
 * user with database-level access but no per-type rights on a given type could read it anyway through the
 * AI chat tool, even though the identical query is rejected on every other query path
 * ({@code POST /api/v1/query/{db}}).
 * <p>
 * The test drives {@link ToolDispatcher} directly (bypassing the AI gateway/HTTP plumbing, which is
 * external infrastructure not under test) with a user granted {@code readRecord} on every type except
 * {@code SecretDoc}. On the vulnerable build {@code query_database} returned the {@code SecretDoc} row; with
 * the principal bound the per-type READ_RECORD gate rejects it, while the same user's query against an
 * authorized type still succeeds.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AiChatToolDispatcherPerTypeAclIT extends BaseGraphServerTest {

  private static final String SCOPED_USER = "aichat-pertype-scoped-user";
  private static final String SCOPED_PWD  = "aichatpertypeuser1";
  private static final String PUBLIC_TYPE = "PublicDoc";
  private static final String SECRET_TYPE = "SecretDoc";

  @Test
  void queryDatabaseToolEnforcesPerTypeAcl() throws Exception {
    testEachServer((serverIndex) -> {
      command(serverIndex, "CREATE VERTEX TYPE " + PUBLIC_TYPE);
      command(serverIndex, "CREATE VERTEX TYPE " + SECRET_TYPE);
      command(serverIndex, "CREATE VERTEX " + PUBLIC_TYPE + " SET x = 1");
      command(serverIndex, "CREATE VERTEX " + SECRET_TYPE + " SET x = 1");

      createScopedUser(serverIndex);
      try {
        final ServerSecurity security = getServer(serverIndex).getSecurity();
        final ServerSecurityUser scopedUser = security.authenticate(SCOPED_USER, SCOPED_PWD, getDatabaseName());

        final ToolDispatcher dispatcher = new ToolDispatcher(getServer(serverIndex), scopedUser, getDatabaseName());

        // Attack: query a per-type forbidden type through the AI chat tool. On the vulnerable build this
        // returned the SecretDoc row (no principal bound -> per-type READ_RECORD check skipped).
        final JSONObject secretResult = new JSONObject(dispatcher.execute("query_database",
            new JSONObject().put("language", "sql").put("command", "SELECT FROM " + SECRET_TYPE)));
        assertThat(secretResult.has("error"))
            .as("query_database on a per-type forbidden type must be rejected").isTrue();

        // Positive control: the same user may query an authorized type - proving the rejection above is
        // per-type authorization, not a blanket tool failure.
        final JSONObject publicResult = new JSONObject(dispatcher.execute("query_database",
            new JSONObject().put("language", "sql").put("command", "SELECT FROM " + PUBLIC_TYPE)));
        assertThat(publicResult.has("error"))
            .as("query_database on an authorized type must succeed").isFalse();
        assertThat(publicResult.getJSONArray("result")).hasSize(1);
      } finally {
        deleteUser(serverIndex, SCOPED_USER);
      }
    });
  }

  private void createScopedUser(final int serverIndex) throws Exception {
    final ServerSecurity security = getServer(serverIndex).getSecurity();

    // Grant readRecord on every type via the "*" default, but explicitly REVOKE all access on SECRET_TYPE.
    // A listed type overrides the "*" default in the per-type map resolution.
    ServerSecurityTestAccess.databaseGroups(security, getDatabaseName()).put("aiChatPerTypeScoped",
        new JSONObject().put("access", new JSONArray())
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))
                .put(SECRET_TYPE, new JSONObject().put("access", new JSONArray()))));
    security.saveGroups();

    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);

    security.createUser(new JSONObject()
        .put("name", SCOPED_USER)
        .put("password", security.encodePassword(SCOPED_PWD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("aiChatPerTypeScoped"))));
  }

  private void deleteUser(final int serverIndex, final String name) {
    final ServerSecurity security = getServer(serverIndex).getSecurity();
    if (security.existsUser(name))
      security.dropUser(name);
  }
}
