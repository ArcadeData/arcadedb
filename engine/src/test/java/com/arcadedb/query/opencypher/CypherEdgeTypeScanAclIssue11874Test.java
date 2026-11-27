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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The scan of a relationship's types (issue #11874) under per-type read permissions.
 * <p>
 * The plan of a labelled source compares the scan with the walk on the vertex counts of the schema, and a count is refused to a user
 * who may not read the type: a type the statement never names must not refuse the statement. And the scan loads an endpoint only
 * for an edge its predicate keeps, so left to itself it would refuse a user who may not read the label of an endpoint only for a
 * value some edge holds, where the walk of that label refuses whatever the value is, as the edge index seek does (issue #12663).
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherEdgeTypeScanAclIssue11874Test {
  private static final String PATH      = "target/databases/CypherEdgeTypeScanAclIssue11874Test";
  private static final String TYPE_SCAN = "MATCH EDGE BY TYPE SCAN";

  /** Statements with a labelled endpoint, written with {@code $v}; the edges of the fixture hold the values 1 to 5 and 11 to 15. */
  private static final String[] SECRET_ENDPOINT = { "MATCH (p:Secret)-[e:K]->(q) WHERE e.w = $v RETURN id(q) AS r",
      "MATCH (q)<-[e:K]-(p:Secret) WHERE e.w = $v RETURN id(q) AS r", "MATCH (q)-[e:K]->(p:Secret) WHERE e.w = $v RETURN id(q) AS r",
      "MATCH (q)-[e:K {w: $v}]->(p:Secret) RETURN id(q) AS r", "MATCH (q:Open)-[e:K]->(p:Secret) WHERE e.w = $v RETURN id(q) AS r" };

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Open");
      database.getSchema().createVertexType("Secret");
      database.getSchema().createVertexType("Rare");
      database.getSchema().createVertexType("Unrelated");
      // no index: the predicate on w is answered by the scan of the type
      database.getSchema().createEdgeType("K");
    });
    database.transaction(() -> {
      final List<MutableVertex> open = new ArrayList<>();
      final List<MutableVertex> secret = new ArrayList<>();
      // more vertices than edges, so that the scan of K costs less than the walk from either label
      for (int i = 0; i < 20; i++) {
        open.add(database.newVertex("Open").set("id", i).save());
        secret.add(database.newVertex("Secret").set("id", i).save());
      }
      database.newVertex("Unrelated").set("id", 0).save();
      database.newVertex("Rare").set("id", 0).save().newEdge("K", open.get(0), "w", 31);
      for (int i = 1; i <= 5; i++) {
        open.get(i).newEdge("K", secret.get(i), "w", i);
        secret.get(i).newEdge("K", open.get(i), "w", i + 10);
        open.get(i).newEdge("K", open.get(i + 1), "w", i + 20);
      }
    });
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void aTypeTheStatementDoesNotNameDoesNotRefuseItsPlanning() {
    deny("Unrelated");

    // planned for the restricted user: no plan of these statements is cached yet. The one vertex of Rare is the cheaper start, a
    // decision the plan takes from the vertex counts of the whole schema, Unrelated included
    assertThat(drain("MATCH (a:Rare)-[e:K]->(b) WHERE e.w = $v RETURN id(b) AS r", 31)).isEqualTo(1);
    assertThat(drain("MATCH (a:Rare)-[e:K]->(b) WHERE e.w = $v RETURN id(b) AS r", 100)).isZero();
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:Rare)-[e:K]->() RETURN sum(e.w) AS s")) {
      assertThat(rs.next().<Number>getProperty("s").longValue()).isEqualTo(31);
    }
    assertThat(drain("MATCH (a:Open)-[e:K]->(b:Open) WHERE e.w = $v RETURN id(b) AS r", 21)).isEqualTo(1);
    assertThat(drain("MATCH (a:Open)-[e:K]->(b) WHERE e.w > $v RETURN id(b) AS r", 20)).isEqualTo(5);
    assertThat(drain("MATCH (a:Secret)-[e:K]->(b) WHERE e.w = $v RETURN id(b) AS r", 13)).isEqualTo(1);
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:Open)-[e:K]->() RETURN sum(e.w) AS s")) {
      assertThat(rs.next().<Number>getProperty("s").longValue()).isEqualTo(1 + 2 + 3 + 4 + 5 + 21 + 22 + 23 + 24 + 25);
    }
    assertThat(explain("MATCH (a:Open)-[e:K]->(b) WHERE e.w > $v RETURN id(b) AS r")).contains(TYPE_SCAN);
  }

  @Test
  void anEndpointLabelTheUserMayNotReadRefusesTheScanWhateverTheValueIs() {
    for (final String statement : SECRET_ENDPOINT)
      assertThat(explain(statement)).as(statement).contains(TYPE_SCAN);

    deny("Secret");

    final List<String> notRefused = new ArrayList<>();
    for (final String statement : SECRET_ENDPOINT)
      // a value an edge of a Secret endpoint holds, one only an edge between Open vertices holds, and one no edge holds
      for (final int value : new int[] { 3, 13, 23, 100 })
        if (!refused(statement, value))
          notRefused.add(statement + " with $v = " + value);
    assertThat(notRefused).as("a denied label refuses the statement, it does not tell whether an edge holds a value").isEmpty();
  }

  @Test
  void anEdgeTypeTheUserMayNotReadRefusesTheScan() {
    final String statement = "MATCH (q)-[e:K]->(p) WHERE e.w = $v RETURN id(q) AS r";
    assertThat(explain(statement)).contains(TYPE_SCAN);

    deny("K");

    for (final int value : new int[] { 3, 100 })
      assertThat(refused(statement, value)).as(statement + " with $v = " + value).isTrue();
    assertThat(catchThrowable(() -> database.query("opencypher", "MATCH ()-[e:K]->() RETURN count(e.w) AS c").next())).isInstanceOf(
        SecurityException.class);
  }

  @Test
  void theTypesTheUserMayReadAreAnsweredWhileAnotherIsDenied() {
    deny("Secret", "Unrelated");

    final String statement = "MATCH (q:Open)-[e:K]->(p:Open) WHERE e.w = $v RETURN id(q) AS r";
    assertThat(drain(statement, 23)).isEqualTo(1);
    assertThat(drain(statement, 100)).isZero();
    assertThat(explain(statement)).contains(TYPE_SCAN);

    // an edge that starts at a Secret vertex is not one of an Open source: the walk from Open never reads that vertex, and
    // the scan must not refuse the statement for it, whatever the isolation level
    final String fromOpen = "MATCH (q:Open)-[e:K]->(p) WHERE e.w = $v RETURN id(q) AS r";
    assertThat(explain(fromOpen)).contains(TYPE_SCAN);
    for (final Database.TRANSACTION_ISOLATION_LEVEL level : Database.TRANSACTION_ISOLATION_LEVEL.values()) {
      database.setTransactionIsolationLevel(level);
      database.begin();
      try {
        assertThat(drain(fromOpen, 13)).as("%s", level).isZero();
        assertThat(drain(fromOpen, 23)).as("%s", level).isEqualTo(1);
        assertThat(drain(statement, 13)).as("%s", level).isZero();
        assertThat(drain(statement, 3)).as("%s", level).isZero();
      } finally {
        database.rollback();
      }
    }
  }

  @Test
  void aUserWhoMayReadEveryTypeAndAStatementWithNoBoundUserAreAnswered() {
    for (final boolean bound : new boolean[] { true, false }) {
      if (bound)
        deny();
      else
        DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);

      for (final String statement : SECRET_ENDPOINT) {
        final boolean secretSource = statement.contains("(p:Secret)-[") || statement.contains("-(p:Secret)");
        assertThat(drain(statement, secretSource ? 13 : 3)).as(statement).isEqualTo(1);
        assertThat(drain(statement, 100)).as(statement).isZero();
      }
    }
  }

  private boolean refused(final String statement, final int value) {
    return catchThrowable(() -> drain(statement, value)) instanceof SecurityException;
  }

  private String explain(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher, Map.of("v", 3))) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  /** The number of rows of the statement. */
  private long drain(final String cypher, final int value) {
    long rows = 0;
    try (final ResultSet rs = database.query("opencypher", cypher, Map.of("v", value))) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  /** Denies the read of every record of the types, and grants everything else. */
  private void deny(final String... typeNames) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : typeNames)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }
}
