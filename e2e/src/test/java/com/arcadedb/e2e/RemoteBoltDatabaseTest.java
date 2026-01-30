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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteBoltDatabaseTest extends ArcadeContainerTemplate {

  private Driver driver;

  @BeforeEach
  void setUp() {
    driver = GraphDatabase.driver(
        "bolt://" + host + ":" + boltPort,
        AuthTokens.basic("root", "playwithdata"),
        Config.builder()
            .withoutEncryption()
            .build()
    );
  }

  @AfterEach
  void tearDown() {
    if (driver != null)
      driver.close();
  }

  @Test
  void testConnection() {
    driver.verifyConnectivity();
  }

  @Test
  void simpleReturnQuery() {
    try (Session session = driver.session(SessionConfig.forDatabase("beer"))) {
      final Result result = session.run("RETURN 1 AS value");
      assertThat(result.hasNext()).isTrue();
      final org.neo4j.driver.Record record = result.next();
      assertThat(record.get("value").asLong()).isEqualTo(1L);
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void queryBeerDatabase() {
    try (Session session = driver.session(SessionConfig.forDatabase("beer"))) {
      final Result result = session.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1");
      assertThat(result.hasNext()).isTrue();
      final org.neo4j.driver.Record record = result.next();
      assertThat(record.get("name").asString()).isNotBlank();
      assertThat(result.hasNext()).isFalse();
    }
  }
}
