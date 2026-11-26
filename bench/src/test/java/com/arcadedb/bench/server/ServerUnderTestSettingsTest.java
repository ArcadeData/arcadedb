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
package com.arcadedb.bench.server;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ServerUnderTestSettingsTest {
  // The remote client talks HTTP/2, and the server cuts an HTTP/2 connection one socketTimeout (30 s) into a statement
  // (issue #12606): at scale M every index build longer than that failed with "EOF reached while reading"
  @Test
  void socketTimeoutCoversTheLongestCase() {
    final Map<String, String> settings = ServerUnderTest.settings(Map.of());

    assertThat(settings.get("arcadedb.network.socketTimeout")).isEqualTo(String.valueOf(30 * 60 * 1000));
    assertThat(settings.get("arcadedb.server.rootPassword")).isEqualTo(ServerUnderTest.PASSWORD);
  }

  @Test
  void anExplicitSettingWins() {
    final Map<String, String> settings = ServerUnderTest.settings(Map.of("arcadedb.network.socketTimeout", "5000"));

    assertThat(settings.get("arcadedb.network.socketTimeout")).isEqualTo("5000");
  }

  @Test
  void workloadSettingsWinOverDefaults() {
    assertThat(ServerUnderTest.settings(Map.of()).get("arcadedb.server.plugins")).isEqualTo("PrometheusMetricsPlugin");
    assertThat(ServerUnderTest.settings(Map.of("arcadedb.server.plugins", "A")).get("arcadedb.server.plugins")).isEqualTo("A");
  }
}
