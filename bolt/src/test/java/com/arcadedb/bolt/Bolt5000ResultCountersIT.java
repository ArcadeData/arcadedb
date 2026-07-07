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
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Values;
import org.neo4j.driver.summary.SummaryCounters;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end wire test: a Bolt write query must populate summary.counters() on the neo4j driver.
 */
public class Bolt5000ResultCountersIT extends BaseGraphServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private Driver getDriver() {
    return GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  @Test
  void writeCountersReflectActualWrites() {
    try (final Driver driver = getDriver();
         final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final SummaryCounters counters = session.run(
          "CREATE (:Beer {name:$n})-[:BREWED_BY]->(:Brewery {name:$b})",
          Values.parameters("n", "IPA", "b", "Acme")).consume().counters();
      assertThat(counters.nodesCreated()).isEqualTo(2);
      assertThat(counters.relationshipsCreated()).isEqualTo(1);
      assertThat(counters.propertiesSet()).isEqualTo(2);
      assertThat(counters.containsUpdates()).isTrue();
    }
  }

  @Test
  void readQueryReportsNoUpdates() {
    try (final Driver driver = getDriver();
         final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      session.run("CREATE (:Thing {id:1})").consume();
      final SummaryCounters counters = session.run("MATCH (n:Thing) RETURN n").consume().counters();
      assertThat(counters.containsUpdates()).isFalse();
      assertThat(counters.nodesCreated()).isZero();
    }
  }

  @Test
  void discardAfterWriteStillReportsCounters() {
    try (final Driver driver = getDriver();
         final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      // A tiny result-set batch size (Session default pull is used by the driver's run()+consume(),
      // which triggers a DISCARD instead of exhausting via PULL for a query producing no records to stream).
      final SummaryCounters counters = session.run("CREATE (:Widget {id:1})").consume().counters();
      assertThat(counters.nodesCreated()).isEqualTo(1);
      assertThat(counters.propertiesSet()).isEqualTo(1);
      assertThat(counters.containsUpdates()).isTrue();
    }
  }
}
