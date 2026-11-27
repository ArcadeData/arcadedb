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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerControlPlane;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every route that creates a user refuses a name that has leading or trailing whitespace, a control character or consecutive
 * whitespace characters. Any other name, an inner single space and non-ASCII text included, is stored as before.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class UserNameCreationRoutesTest extends BaseGraphServerTest {

  private static final String       PASSWORD       = "TestPass123!";
  private static final List<String> REFUSED_NAMES  = List.of("reader ", " reader", "reader\t", "re  ader", "reader\n", "re\u0000ader", "reader \t");
  private static final List<String> ACCEPTED_NAMES = List.of("jane.doe@example.com", "Mary Ann", "Zo\u00eb", "o'brien");

  @AfterEach
  void dropTheNamesUsed() {
    final ServerSecurity security = getServer(0).getSecurity();
    for (final String name : REFUSED_NAMES)
      dropQuietly(security, name);
    for (final String name : ACCEPTED_NAMES)
      dropQuietly(security, name);
  }

  @Test
  void theLocalCreationRefusesTheName() {
    final ServerSecurity security = getServer(0).getSecurity();

    for (final String name : REFUSED_NAMES) {
      assertThatThrownBy(() -> security.createUser(userJson(name))).as(printable(name)).isInstanceOf(IllegalArgumentException.class);
      assertThat(security.existsUser(name)).as(printable(name)).isFalse();
    }
  }

  @Test
  void theClusterWideCreationRefusesTheName() {
    final ServerSecurity security = getServer(0).getSecurity();

    for (final String name : REFUSED_NAMES) {
      assertThatThrownBy(() -> security.createUserClusterWide(userJson(name))).as(printable(name))
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(security.existsUser(name)).as(printable(name)).isFalse();
    }
  }

  @Test
  void theSecurityManagerEntryPointRefusesTheName() {
    final ServerSecurity security = getServer(0).getSecurity();

    for (final String name : REFUSED_NAMES) {
      assertThatThrownBy(() -> security.createUser(name, PASSWORD)).as(printable(name)).isInstanceOf(IllegalArgumentException.class);
      assertThat(security.existsUser(name)).as(printable(name)).isFalse();
    }
  }

  @Test
  void theControlPlaneCreationRefusesTheName() {
    final ServerControlPlane controlPlane = new ServerControlPlane(getServer(0));

    for (final String name : REFUSED_NAMES) {
      assertThatThrownBy(() -> controlPlane.createUser(new JSONObject().put("name", name).put("password", PASSWORD)))
          .as(printable(name)).isInstanceOf(IllegalArgumentException.class);
      assertThat(getServer(0).getSecurity().existsUser(name)).as(printable(name)).isFalse();
    }
  }

  @Test
  void theUserRouteRefusesTheName() throws Exception {
    for (final String name : REFUSED_NAMES) {
      assertThat(post("/api/v1/server/users", new JSONObject().put("name", name).put("password", PASSWORD))).as(printable(name))
          .isEqualTo(400);
      assertThat(getServer(0).getSecurity().existsUser(name)).as(printable(name)).isFalse();
    }
  }

  @Test
  void theServerCommandRefusesTheName() throws Exception {
    for (final String name : REFUSED_NAMES) {
      final String command = "create user " + new JSONObject().put("name", name).put("password", PASSWORD);
      assertThat(post("/api/v1/server", new JSONObject().put("command", command))).as(printable(name)).isEqualTo(400);
      assertThat(getServer(0).getSecurity().existsUser(name)).as(printable(name)).isFalse();
    }
  }

  @Test
  void theOpenCypherStatementRefusesTheName() {
    final Database database = getServerDatabase(0, getDatabaseName());

    for (final String name : List.of("reader ", " reader", "re  ader")) {
      assertThatThrownBy(() -> database.command("opencypher", "CREATE USER `" + name + "` SET PASSWORD '" + PASSWORD + "'"))
          .as(printable(name)).isInstanceOf(RuntimeException.class);
      assertThat(getServer(0).getSecurity().existsUser(name)).as(printable(name)).isFalse();
    }
  }

  @Test
  void anOrdinaryNameIsStoredAsBefore() throws Exception {
    final ServerSecurity security = getServer(0).getSecurity();
    final Database database = getServerDatabase(0, getDatabaseName());

    security.createUser(userJson("jane.doe@example.com"));
    assertThat(security.existsUser("jane.doe@example.com")).isTrue();

    assertThat(post("/api/v1/server/users", new JSONObject().put("name", "Mary Ann").put("password", PASSWORD))).isEqualTo(201);
    assertThat(security.existsUser("Mary Ann")).isTrue();

    database.command("opencypher", "CREATE USER `Zo\u00eb` SET PASSWORD '" + PASSWORD + "'");
    assertThat(security.existsUser("Zo\u00eb")).isTrue();

    assertThat(post("/api/v1/server",
        new JSONObject().put("command", "create user " + new JSONObject().put("name", "o'brien").put("password", PASSWORD))))
        .isEqualTo(200);
    assertThat(security.existsUser("o'brien")).isTrue();

    assertThat(security.getUsers()).doesNotContainAnyElementsOf(Set.copyOf(REFUSED_NAMES));
  }

  private JSONObject userJson(final String name) {
    return new JSONObject().put("name", name).put("password", getServer(0).getSecurity().encodePassword(PASSWORD))
        .put("databases", new JSONObject());
  }

  private int post(final String path, final JSONObject body) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(getServerHttpUrl(0, path)).toURL().openConnection();
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Authorization",
          "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
      formatPayload(connection, body);
      connection.connect();
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private static void dropQuietly(final ServerSecurity security, final String name) {
    try {
      security.dropUser(name);
    } catch (final Exception ignored) {
      // the name was never stored: the point of the test
    }
  }

  private static String printable(final String name) {
    return name.replace("\t", "\\t").replace("\n", "\\n").replace("\u0000", "\\0").replace(' ', '_');
  }
}
