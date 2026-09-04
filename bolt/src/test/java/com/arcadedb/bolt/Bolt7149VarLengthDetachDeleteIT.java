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
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end wire test for issue #7149: a 2-hop variable-length traversal whose rows repeat the same node, followed
 * by {@code SET} and {@code DETACH DELETE} on that node, answered a single Bolt client with
 * {@code Neo.TransientError.Transaction.DeadlockDetected} - telling the driver to retry a statement that could
 * never succeed, since there was no second transaction to conflict with.
 * <p>
 * The wire is the point of this test: the engine-level exception that produced the report is a
 * {@code ConcurrentModificationException}, and only the Bolt classifier turns that into a transient status. The
 * defects behind it are pinned in the engine module -
 * {@link com.arcadedb.query.opencypher.CypherVarLengthDetachDeleteIssue7149Test} and
 * {@link com.arcadedb.database.Issue7149UpdateAfterDeleteInSameTxTest}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Bolt7149VarLengthDetachDeleteIT extends BaseGraphServerTest {
  private static final String EDGES = "[[14,126],[72,112],[13,11],[57,54],[2,105],[64,13],[123,26],[60,75],[101,2],[70,51],"
      + "[59,2],[33,113],[73,52],[94,46],[73,93],[93,104],[64,41],[33,38],[27,72],[118,118],[85,24],[85,67],[51,89],[19,38],"
      + "[113,22],[59,78],[0,33],[76,76],[78,64],[15,16],[95,95],[9,38],[2,2],[67,2],[126,87],[42,86],[54,54],[48,101],"
      + "[107,50],[85,39],[116,13],[101,47],[5,84],[123,81],[93,26],[107,127],[57,116],[10,57],[65,65],[101,81],[22,22],"
      + "[65,65],[117,116],[27,42],[116,116],[26,93],[41,5],[35,38],[57,71],[86,47],[67,84],[107,124],[98,110],[67,28],"
      + "[48,48],[72,17],[52,95],[123,2],[93,127],[52,65],[98,105],[13,5]]";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
    // The reporter ran a stock container: one bucket per type, so every node of a type shares the same page.
    GlobalConfiguration.TYPE_DEFAULT_BUCKETS.setValue(1);
  }

  @Override
  protected void populateDatabase() {
    // The issue reproduces on a clean database: the Cypher setup below creates every type it needs.
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

  private void setUpGraph(final Session s) {
    s.run("UNWIND range(0, 127) AS id CREATE (:N {id: id})").consume();
    s.run("UNWIND " + EDGES + " AS e MATCH (a {id: e[0]}), (b {id: e[1]}) CREATE (a)-[:R]->(b)").consume();
  }

  private long countSurvivingNodes(final Session s) {
    return s.run("MATCH (n:N) RETURN count(n) AS c").single().get("c").asLong();
  }

  @Test
  void controlWithoutDetachDeleteReturnsEveryTwoHopPath() {
    try (final Driver driver = getDriver()) {
      try (final Session s = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        setUpGraph(s);

        final List<Record> rows = s.run("""
            MERGE (:DeadlockProbe {id: 999995})
            MATCH p0 = (n2) -[*2]-()
            SET n2.marker = 'probe'
            RETURN null AS alias0""").list();

        assertThat(rows).as("the issue's control case returns the 2-hop paths").hasSize(256);
      }
    }
  }

  @Test
  void varLengthTraversalThenDetachDeleteDoesNotReportADeadlock() {
    try (final Driver driver = getDriver()) {
      try (final Session s = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        setUpGraph(s);

        // Pre-fix: org.neo4j.driver.exceptions.TransientException, code
        // Neo.TransientError.Transaction.DeadlockDetected. The records are consumed here (.list()), as the issue
        // notes a lazy unconsumed result can report success before the server would have reported the failure.
        final List<Record> rows = s.run("""
            MERGE (:DeadlockProbe {id: 999995})
            MATCH p0 = (n2) -[*2]-()
            SET n2.marker = 'probe'
            DETACH DELETE n2
            CALL merge.node(['ProbePerson'], {name: 'ArcadeGenerated'}, {name: 'ArcadeGenerated'}) YIELD node AS alias3
            RETURN null AS alias0""").list();

        assertThat(rows).as("one projected row per traversal row").hasSize(256);
        assertThat(countSurvivingNodes(s)).as("only the nodes no 2-hop path reaches survive").isEqualTo(71);
      }
    }
  }
}
