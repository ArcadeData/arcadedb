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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the Ratis-migration packaging gotcha: when HA is requested (explicitly via
 * {@code ha.enabled=true} or implicitly via a non-blank {@code ha.serverList}) but the
 * {@code arcadedb-ha-raft} module is not on the classpath, no {@link HAServerPlugin} is discovered
 * and the node silently runs standalone. The server must warn loudly instead.
 * <p>
 * The {@code server} module does not depend on {@code arcadedb-ha-raft}, so a server booted from
 * this test reproduces exactly an embedded application that forgot to add the HA dependency.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ServerHAPluginMissingWarningTest extends StaticBaseServerTest {
  private ArcadeDBServer server;

  @BeforeEach
  public void beginTest() {
    super.beginTest();
  }

  @AfterEach
  public void endTest() {
    if (server != null && server.isStarted())
      server.stop();
    super.endTest();
  }

  @Test
  void warnsWhenHAEnabledButPluginMissing() {
    final ContextConfiguration config = baseConfig();
    config.setValue(GlobalConfiguration.HA_ENABLED, true);
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "node1,node2,node3");

    server = new ArcadeDBServer(config);
    server.start();

    // No arcadedb-ha-raft on the classpath -> no HA plugin registered.
    assertThat(server.getHA()).isNull();

    final JSONObject warning = findHAWarning(server);
    assertThat(warning).as("expected a WARNING event for the missing HA plugin").isNotNull();
    assertThat(warning.getString("message")).contains("arcadedb-ha-raft");
  }

  @Test
  void warnsWhenHAImplicitlyEnabledByServerList() {
    final ContextConfiguration config = baseConfig();
    // HA not explicitly enabled, but a non-blank server list implies HA intent.
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "node1,node2,node3");

    server = new ArcadeDBServer(config);
    server.start();

    assertThat(server.getHA()).isNull();
    assertThat(findHAWarning(server)).as("expected a WARNING event when HA is implicitly enabled").isNotNull();
  }

  @Test
  void doesNotWarnWhenHADisabled() {
    final ContextConfiguration config = baseConfig();

    server = new ArcadeDBServer(config);
    server.start();

    assertThat(server.getHA()).isNull();
    assertThat(findHAWarning(server)).as("no HA warning expected when HA is not requested").isNull();
  }

  private static ContextConfiguration baseConfig() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
    return config;
  }

  private static JSONObject findHAWarning(final ArcadeDBServer server) {
    final JSONArray events = server.getEventLog().getCurrentEvents();
    for (int i = 0; i < events.length(); i++) {
      final JSONObject event = events.getJSONObject(i);
      if ("WARNING".equals(event.getString("type", "")) && "HA".equals(event.getString("component", "")))
        return event;
    }
    return null;
  }
}
