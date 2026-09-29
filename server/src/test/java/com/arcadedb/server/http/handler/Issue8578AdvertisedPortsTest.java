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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ServerPlugin;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8578, review of PR #8680: {@code GET /api/v1/server?mode=cluster} publishes the client-facing ports of the active
 * plugins. A plugin that throws must not fail the route, a port that is null or not positive is dropped, and a service name
 * two plugins both advertise keeps its first owner.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8578AdvertisedPortsTest {

  @Test
  void publishesTheBoundPortOfEachService() {
    final JSONObject ports = GetServerHandler.buildAdvertisedPorts(List.of(plugin(Map.of("gremlin", 18182)), plugin(Map.of("mcp", 9000))));

    assertThat(ports.keySet()).containsExactlyInAnyOrder("gremlin", "mcp");
    assertThat(ports.getInt("gremlin")).isEqualTo(18182);
  }

  @Test
  void dropsPortsThatAreNullOrNotPositive() {
    final Map<String, Integer> raw = new HashMap<>();
    raw.put("unbound", 0);
    raw.put("negative", -1);
    raw.put("missing", null);
    raw.put("ok", 7);

    assertThat(GetServerHandler.buildAdvertisedPorts(List.of(plugin(raw))).keySet()).containsExactly("ok");
  }

  @Test
  void aPluginThatThrowsDoesNotFailTheRoute() {
    final ServerPlugin broken = new ServerPlugin() {
      @Override
      public void startService() {
      }

      @Override
      public Map<String, Integer> getAdvertisedPorts() {
        throw new IllegalStateException("boom");
      }
    };

    final JSONObject ports = GetServerHandler.buildAdvertisedPorts(List.of(broken, plugin(Map.of("gremlin", 1234))));

    assertThat(ports.keySet()).containsExactly("gremlin");
  }

  @Test
  void aServiceNameKeepsItsFirstOwner() {
    final JSONObject ports = GetServerHandler.buildAdvertisedPorts(List.of(plugin(Map.of("gremlin", 1111)), plugin(Map.of("gremlin", 2222))));

    assertThat(ports.getInt("gremlin")).isEqualTo(1111);
  }

  private static ServerPlugin plugin(final Map<String, Integer> ports) {
    return new ServerPlugin() {
      @Override
      public void startService() {
      }

      @Override
      public Map<String, Integer> getAdvertisedPorts() {
        return ports;
      }
    };
  }
}
