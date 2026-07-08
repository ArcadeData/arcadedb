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
package com.arcadedb.e2e;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Exercises the A1 java driver-version range (conformance spec, issue #4883)
 * against the {@code oldest-supported-4.x} band. Uses only cross-version-stable
 * driver API (no {@code executeWrite}/{@code lastBookmarks}) so the same source
 * compiles and runs under both the 6.x baseline and the 4.4 band.
 * <p>
 * Under {@code -Pbolt-driver-legacy} the {@code neo4j-java-driver} is resolved
 * to the 4.4 line and the full {@link RemoteBoltDatabaseIT} (which uses 5.x+
 * driver API) is excluded from compilation.
 */
class RemoteBoltLegacyDriverIT extends ArcadeContainerTemplate {

  private Driver newDriver() {
    return GraphDatabase.driver("bolt://" + host + ":" + boltPort,
        AuthTokens.basic("root", "playwithdata"),
        Config.builder().withoutEncryption().build());
  }

  @Test
  @DisplayName("[CONN-001] Connect and read on the 4.4 driver band")
  void conn001_legacyConnectAndRead() {
    try (final Driver d = newDriver()) {
      d.verifyConnectivity();
      try (final Session s = d.session(SessionConfig.forDatabase("beer"))) {
        assertThat(s.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1")
            .single().get("name").asString()).isNotBlank();
      }
    }
  }

  @Test
  @DisplayName("[PROTO-001] Parameterized read negotiates on the 4.4 driver band")
  void proto001_legacyParameterized() {
    try (final Driver d = newDriver();
        final Session s = d.session(SessionConfig.forDatabase("beer"))) {
      assertThat(s.run("RETURN $v AS v", Map.of("v", 7L)).single().get("v").asLong()).isEqualTo(7L);
    }
  }

  @Test
  @DisplayName("[ERR-001] Syntax error code on the 4.4 driver band")
  void err001_legacySyntaxErrorCode() {
    try (final Driver d = newDriver();
        final Session s = d.session(SessionConfig.forDatabase("beer"))) {
      assertThatThrownBy(() -> s.run("MATCH (n RETURN n").consume())
          .isInstanceOf(ClientException.class);
    }
  }
}
