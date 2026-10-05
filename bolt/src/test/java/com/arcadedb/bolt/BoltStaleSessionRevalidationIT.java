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
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * An open BOLT connection must stop working when its user is deleted, loses the database grant or has its password
 * rotated: it used to keep the access it had at LOGON time while HTTP refused the same credentials.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class BoltStaleSessionRevalidationIT extends BaseBoltServerTest {
  private static final String USER     = "boltStaleUser";
  private static final String PASSWORD = "boltStalePassword1";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(USER) != null)
      security.dropUser(USER);
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void deletedUserIsCutOff() {
    run(security -> security.dropUser(USER));
  }

  @Test
  void revokedDatabaseGrantIsCutOff() {
    run(security -> security.updateUser(new JSONObject().put("name", USER).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject())));
  }

  @Test
  void rotatedPasswordIsCutOff() {
    run(security -> security.updateUser(
        new JSONObject().put("name", USER).put("password", security.encodePassword("another-Password-2"))
            .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin")))));
  }

  private void run(final Consumer<ServerSecurity> change) {
    final ServerSecurity security = getServer(0).getSecurity();
    security.createUser(new JSONObject().put("name", USER).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin"))));

    try (final Driver driver = GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic(USER, PASSWORD),
        Config.builder().withoutEncryption().build());
        final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      session.run("CREATE (:StaleDoc {name:'before'})").consume();

      change.accept(security);

      final Throwable thrown = catchThrowable(() -> session.run("CREATE (:StaleDoc {name:'after'})").consume());
      assertThat(thrown).as("a connection whose user was revoked must be refused").isNotNull();
      assertThat(getServerDatabase(0, getDatabaseName()).countType("StaleDoc", false)).isEqualTo(1L);
    }
  }
}
