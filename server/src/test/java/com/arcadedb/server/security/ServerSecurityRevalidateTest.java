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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link ServerSecurity#revalidate(ServerSecurityUser)}: what a long-lived wire connection sees of a change made to its
 * user after it authenticated.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ServerSecurityRevalidateTest {
  private static final String CONFIG_PATH = "target/test-security-revalidate";
  private static final String USER        = "wireUser";
  private static final String PASSWORD    = "wirePassword1";

  private ServerSecurity security;

  @BeforeEach
  void setUp() {
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.setValue(1000);
    final File dir = new File(CONFIG_PATH);
    if (dir.exists())
      FileUtils.deleteRecursively(dir);
    assertThat(dir.mkdirs()).isTrue();

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getDatabaseNames()).thenReturn(Set.of());
    security = new ServerSecurity(server, new ContextConfiguration(), CONFIG_PATH);
    when(server.getSecurity()).thenReturn(security);
    security.startService();
    security.createUser(userConfiguration(security.encodePassword(PASSWORD), "db1"));
  }

  @AfterEach
  void tearDown() {
    security.stopService();
    GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.reset();
    FileUtils.deleteRecursively(new File(CONFIG_PATH));
  }

  @Test
  void unchangedUserIsTheSameInstance() {
    final ServerSecurityUser held = security.authenticate(USER, PASSWORD, "db1");
    assertThat(security.revalidate(held)).isSameAs(held);
  }

  @Test
  void deletedUserIsRefused() {
    final ServerSecurityUser held = security.authenticate(USER, PASSWORD, "db1");
    security.dropUserLocally(USER);
    assertThatThrownBy(() -> security.revalidate(held)).isInstanceOf(ServerSecurityException.class);
  }

  @Test
  void changedPasswordIsRefused() {
    final ServerSecurityUser held = security.authenticate(USER, PASSWORD, "db1");
    security.updateUser(userConfiguration(security.encodePassword("anotherPassword2"), "db1"));
    assertThatThrownBy(() -> security.revalidate(held)).isInstanceOf(ServerSecurityException.class);
  }

  @Test
  void changeKeepingThePasswordSurvivesAndCarriesTheNewGrants() {
    final ServerSecurityUser held = security.authenticate(USER, PASSWORD, "db1");
    assertThat(held.canAccessToDatabase("db1")).isTrue();

    // same stored hash, different grants: the connection must survive, on the new object
    security.updateUser(userConfiguration(held.getPassword(), "db2"));

    final ServerSecurityUser current = security.revalidate(held);
    assertThat(current).isNotSameAs(held);
    assertThat(current.canAccessToDatabase("db1")).isFalse();
    assertThat(current.canAccessToDatabase("db2")).isTrue();
  }

  private static JSONObject userConfiguration(final String password, final String database) {
    return new JSONObject().put("name", USER).put("password", password)
        .put("databases", new JSONObject().put(database, new JSONArray().put("admin")));
  }
}
