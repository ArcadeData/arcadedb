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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.ClientException;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10190 over the wire: a Bolt auto-commit query ({@code session.run(...)} with no explicit transaction) is one
 * transaction, as an explicit Bolt transaction and an HTTP command already were. Each Cypher write step used to commit
 * on its own, so a failing statement kept its earlier writes, a MERGE over several pairs failed as a deadlock, and a
 * label round trip never answered.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10190BoltAutoCommitAtomicityIT extends BaseBoltServerTest {

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
    return GraphDatabase.driver(getServerBoltUrl(), AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
  }

  @Test
  void aFailingStatementLeavesNothingBehind() {
    try (final Driver driver = getDriver();
        final Session s = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      assertThatThrownBy(() -> s.run("CREATE (:A10190 {k: 1}) WITH 1 AS z RETURN 1 / (z - 1) AS e").consume())
          .isInstanceOf(ClientException.class).hasMessageContaining("/ by zero");
      assertThat(s.run("MATCH (n:A10190) RETURN count(n) AS c").single().get("c").asLong()).isZero();

      s.run("CREATE (:B10190 {k: 0}), (:B10190 {k: 1})").consume();
      assertThatThrownBy(() -> s.run("MATCH (n:B10190) WITH n ORDER BY n.k SET n.i = 10 / (n.k - 1)").consume())
          .isInstanceOf(ClientException.class).hasMessageContaining("/ by zero");
      assertThat(s.run("MATCH (n:B10190) WHERE n.i IS NOT NULL RETURN count(n) AS c").single().get("c").asLong()).isZero();
    }
  }

  @Test
  void aMergeOverPairsCreatesEveryRelationship() {
    try (final Driver driver = getDriver();
        final Session s = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      s.run("CREATE (:C10190 {k: 0}), (:C10190 {k: 1}), (:C10190 {k: 2})").consume();
      s.run("MATCH (a:C10190), (b:C10190) WHERE a.k < b.k MERGE (a)-[:S10190]->(b)").consume();
      assertThat(s.run("MATCH (:C10190)-[r:S10190]->(:C10190) RETURN count(r) AS c").single().get("c").asLong()).isEqualTo(3L);
    }
  }

  @Test
  @Timeout(value = 60, unit = TimeUnit.SECONDS) // A HANG DETECTOR: THE STATEMENT USED TO NEVER ANSWER
  void aLabelRoundTripAnswersAndKeepsTheLabel() {
    try (final Driver driver = getDriver();
        final Session s = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      s.run("CREATE (:D10190 {k: 0})").consume();
      final List<Object> labels = s.run("MATCH (n:D10190) REMOVE n:D10190 SET n:D10190 RETURN labels(n) AS l").single()
          .get("l").asList();
      assertThat(labels).containsExactly("D10190");
      assertThat(s.run("MATCH (n:D10190) RETURN count(n) AS c").single().get("c").asLong()).isEqualTo(1L);
    }
  }
}
