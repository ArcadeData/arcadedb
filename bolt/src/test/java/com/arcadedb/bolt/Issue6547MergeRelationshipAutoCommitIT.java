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
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end wire test for issue #6547: against ArcadeDB 26.8.1, {@code merge.relationship(a, 'KNOWS', {}, {...}, b)}
 * called from a top-level {@code MATCH ... CALL ... YIELD ... RETURN} autocommit query over Bolt failed with
 * {@code TransactionException: Transaction not active}, while the identical Cypher succeeded against the HTTP
 * command endpoint. This is the literal reproduction from the issue report: two autocommit {@code CREATE}
 * statements followed by the {@code MATCH}/{@code CALL merge.relationship} statement, each sent as its own
 * {@code session.run(...)} exactly as a Neo4j Bolt driver would.
 * <p>
 * It does not reproduce on this branch: {@code CallStep} already auto-commits a write {@code CypherProcedure}
 * invoked with no transaction already open (commit 420249f2d, issue #6073), and that fix landed 2026-08-12,
 * after the {@code 26.8.1} tag (2026-08-03) the report was filed against. This test locks that behavior in as a
 * permanent regression guard, the same way {@link Bolt6367SelfLoopMergeIT} does for issue #6367.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6547MergeRelationshipAutoCommitIT extends BaseGraphServerTest {

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
  void mergeRelationshipAutocommitsOverBoltWithoutTransactionNotActive() {
    try (final Driver driver = getDriver()) {
      try (final Session s = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        s.run("CREATE (a:Human6547 {id: 'a'})").consume();
        s.run("CREATE (b:Human6547 {id: 'b'})").consume();

        final Result result = s.run(
            "MATCH (a:Human6547 {id: 'a'}), (b:Human6547 {id: 'b'}) "
                + "CALL merge.relationship(a, 'KNOWS', {}, {runId: 'control'}, b) "
                + "YIELD rel "
                + "RETURN type(rel) AS relationship_type");

        final Record record = result.single();
        assertThat(record.get("relationship_type").asString()).isEqualTo("KNOWS");
      }
    }
  }

  /**
   * The issue report also calls out that the failure occurs when the two vertices are selected with
   * {@code MATCH (a), (b) WITH a, b LIMIT 1} rather than a direct label/property match.
   */
  @Test
  void mergeRelationshipAutocommitsOverBoltWithWithLimitSelection() {
    try (final Driver driver = getDriver()) {
      try (final Session s = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        s.run("CREATE (a:Human6547b {id: 'a'})").consume();
        s.run("CREATE (b:Human6547b {id: 'b'})").consume();

        final Result result = s.run(
            "MATCH (a:Human6547b {id: 'a'}), (b:Human6547b {id: 'b'}) WITH a, b LIMIT 1 "
                + "CALL merge.relationship(a, 'KNOWS', {}, {runId: 'control'}, b) "
                + "YIELD rel "
                + "RETURN type(rel) AS relationship_type");

        final Record record = result.single();
        assertThat(record.get("relationship_type").asString()).isEqualTo("KNOWS");
      }
    }
  }
}
