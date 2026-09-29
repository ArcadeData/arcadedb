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

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8625: the scan that answers the incoming side of a unidirectional edge type is an internal read, so the
 * {@code resultSetLimit} of the user running the query must not cut it short - a partial scan would answer part of
 * the edges with no error, which is the failure the issue is about.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8625UnidirectionalLookupResultSetLimitIT extends BaseGraphServerTest {
  private static final String GROUP   = "issue8625capped";
  private static final String USER    = "issue8625reader";
  private static final String PWD     = "issue8625pwd";
  private static final int    LIMIT   = 5;
  private static final int    SOURCES = 20;

  @Test
  void aCappedUserStillGetsEveryIncomingEdge() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createVertexType("Issue8625Q");
    database.getSchema().createVertexType("Issue8625T");
    database.getSchema().buildEdgeType().withName("Issue8625X").withBidirectional(false).create();
    database.transaction(() -> {
      final MutableVertex tag = database.newVertex("Issue8625T").set("name", "t").save();
      for (int i = 0; i < SOURCES; i++)
        database.newVertex("Issue8625Q").set("qid", i).save().newEdge("Issue8625X", tag);
    });

    final ServerSecurity security = getServer(0).getSecurity();
    security.getDatabaseGroupsConfiguration(getDatabaseName()).put(GROUP, new JSONObject()
        .put("resultSetLimit", LIMIT)
        .put("types", new JSONObject().put("*", new JSONObject().put("access", new JSONArray().put("readRecord")))));
    security.saveGroups();
    if (security.existsUser(USER))
      security.dropUser(USER);
    security.createUser(new JSONObject().put("name", USER).put("password", security.encodePassword(PWD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put(GROUP))));

    try (final RemoteDatabase remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), USER, PWD)) {
      // Precondition: the cap bites on an ordinary scan
      long scanned = 0;
      try (final ResultSet rs = remote.query("sql", "SELECT FROM Issue8625Q")) {
        while (rs.hasNext()) {
          rs.next();
          ++scanned;
        }
      }
      assertThat(scanned).isEqualTo(LIMIT);

      // The source is left unlabelled, so the hop is walked from the bound target and answered by the scan
      try (final ResultSet rs = remote.query("opencypher",
          "MATCH (t:Issue8625T {name: 't'}) MATCH (t)<-[:Issue8625X]-(q) RETURN count(q) AS n")) {
        assertThat(((Number) rs.next().getProperty("n")).longValue())
            .as("the lookup must scan every edge, whatever the user's resultSetLimit").isEqualTo(SOURCES);
      }
    } finally {
      security.dropUser(USER);
      security.getDatabaseGroupsConfiguration(getDatabaseName()).remove(GROUP);
      security.saveGroups();
    }
  }
}
