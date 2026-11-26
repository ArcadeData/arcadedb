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
package com.arcadedb.server.ai;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurity;
import com.arcadedb.server.security.ServerSecurityTestAccess;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The refusal the engine raises for a record or a type the bound user may not read names the schema objects of the database. The
 * assistant's {@code query_database} tool answered it in every server mode, while every other surface answers the placeholder in
 * production mode and writes the detail to the server log.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ToolDispatcherRefusalTextByServerModeTest extends BaseGraphServerTest {
  private static final String SCOPED_USER = "assistant-scoped-reader";
  private static final String SCOPED_PWD  = "assistantscopedreader1";
  private static final String CLOSED_TYPE = "ClosedDoc";

  private ToolDispatcher dispatcher;

  @BeforeEach
  void createScopedUser() {
    final Database database = getServerDatabase(0, getDatabaseName());
    if (!database.getSchema().existsType(CLOSED_TYPE)) {
      database.command("sql", "CREATE DOCUMENT TYPE " + CLOSED_TYPE);
      database.transaction(() -> database.newDocument(CLOSED_TYPE).set("v", 1).save());
    }

    final ServerSecurity security = getServer(0).getSecurity();
    ServerSecurityTestAccess.databaseGroups(security, getDatabaseName()).put("assistantScopedReaders",
        new JSONObject().put("access", new JSONArray())
            .put("types", new JSONObject()
                .put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))
                .put(CLOSED_TYPE, new JSONObject().put("access", new JSONArray()))));
    security.saveGroups();

    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);
    security.createUser(new JSONObject()
        .put("name", SCOPED_USER)
        .put("password", security.encodePassword(SCOPED_PWD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("assistantScopedReaders"))));

    final ServerSecurityUser scoped = security.authenticate(SCOPED_USER, SCOPED_PWD, null);
    dispatcher = new ToolDispatcher(getServer(0), scoped, getDatabaseName());
  }

  @AfterEach
  void dropScopedUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.existsUser(SCOPED_USER))
      security.dropUser(SCOPED_USER);
  }

  @Test
  void refusalOnARecordReadIsConcealedInProductionMode() throws Exception {
    final String rid = "#" + closedBucketId() + ":0";

    final String error = withMode("production", () -> errorOf("SELECT FROM " + rid));

    assertThat(error).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(error).doesNotContain(CLOSED_TYPE);
  }

  @Test
  void refusalOnATypeReadIsConcealedInProductionMode() throws Exception {
    final String error = withMode("production", () -> errorOf("SELECT FROM " + CLOSED_TYPE));

    assertThat(error).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }

  @Test
  void developmentModeKeepsTheRefusalText() throws Exception {
    final String rid = "#" + closedBucketId() + ":0";

    final String error = withMode("development", () -> errorOf("SELECT FROM " + rid));

    assertThat(error).contains(CLOSED_TYPE);
  }

  private String errorOf(final String command) {
    final JSONObject result = new JSONObject(dispatcher.execute("query_database", new JSONObject()
        .put("language", "sql")
        .put("command", command)));
    assertThat(result.has("error")).isTrue();
    return result.getString("error");
  }

  private int closedBucketId() {
    return getServerDatabase(0, getDatabaseName()).getSchema().getType(CLOSED_TYPE).getBucketIds(false).getFirst();
  }

  private <T> T withMode(final String mode, final Callable<T> work) throws Exception {
    final Object previous = getServer(0).getConfiguration().getValue(GlobalConfiguration.SERVER_MODE);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, mode);
    try {
      return work.call();
    } finally {
      getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_MODE, previous);
    }
  }
}
