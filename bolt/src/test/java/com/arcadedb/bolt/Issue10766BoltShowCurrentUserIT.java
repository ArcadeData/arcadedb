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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10766: {@code SHOW CURRENT USER} and {@code CALL dbms.showCurrentUser()} answered the constant {@code roles=["admin"]}
 * for every principal. The roles column must be a function of the bound principal, so the test runs as users holding known,
 * non-admin groups: a test that only runs as root cannot tell a correct answer from the constant.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10766BoltShowCurrentUserIT extends BaseBoltServerTest {
  private static final String READER_USER   = "issue10766Reader";
  private static final String TWO_GROUPS    = "issue10766TwoGroups";
  private static final String PASSWORD      = "issue10766Password";
  private static final String READER_GROUP  = "issue10766reader";
  private static final String WRITER_GROUP  = "issue10766writer";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    final ArcadeDBServer server = getServer(0);
    if (server != null && server.isStarted()) {
      final ServerSecurity security = server.getSecurity();
      for (final String user : new String[] { READER_USER, TWO_GROUPS })
        if (security.getUser(user) != null)
          security.dropUser(user);
      security.deleteGroup(getDatabaseName(), READER_GROUP);
      security.deleteGroup(getDatabaseName(), WRITER_GROUP);
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void rolesAreTheGroupsTheUserHolds() {
    getServerDatabase(0, getDatabaseName()).getSchema().getOrCreateVertexType("V");

    final ServerSecurity security = getServer(0).getSecurity();
    security.saveGroup(getDatabaseName(), READER_GROUP, new JSONObject().put("types",
        new JSONObject().put("V", new JSONObject().put("access", new JSONArray(new String[] { "readRecord" })))));
    security.saveGroup(getDatabaseName(), WRITER_GROUP, new JSONObject().put("types",
        new JSONObject().put("V", new JSONObject().put("access", new JSONArray(new String[] { "createRecord" })))));
    createUser(security, READER_USER, READER_GROUP);
    createUser(security, TWO_GROUPS, WRITER_GROUP, READER_GROUP);

    for (final String query : new String[] { "SHOW CURRENT USER", "CALL dbms.showCurrentUser()" }) {
      final Record reader = currentUser(READER_USER, PASSWORD, query);
      assertThat(reader.get("user").asString()).isEqualTo(READER_USER);
      assertThat(roles(reader)).as(query).containsExactly(READER_GROUP);
      assertThat(reader.get("passwordChangeRequired").asBoolean()).isFalse();
      assertThat(reader.get("suspended").asBoolean()).isFalse();

      // every group, sorted, so the answer is stable whatever order the configuration lists them in
      assertThat(roles(currentUser(TWO_GROUPS, PASSWORD, query))).as(query).containsExactly(READER_GROUP, WRITER_GROUP);

      // root holds ArcadeDB's admin group, which is Neo4j's admin role
      final Record root = currentUser("root", DEFAULT_PASSWORD_FOR_TESTS, query);
      assertThat(root.get("user").asString()).isEqualTo("root");
      assertThat(roles(root)).as(query).containsExactly("admin");
    }
  }

  private void createUser(final ServerSecurity security, final String name, final String... groups) {
    security.createUser(new JSONObject().put("name", name).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray(groups))));
  }

  private Record currentUser(final String user, final String password, final String query) {
    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic(user, password),
        Config.builder().withoutEncryption().build());
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final List<Record> rows = session.run(query).list();
      assertThat(rows).hasSize(1);
      return rows.getFirst();
    }
  }

  private static List<String> roles(final Record record) {
    return record.get("roles").asList(Value::asString);
  }
}
