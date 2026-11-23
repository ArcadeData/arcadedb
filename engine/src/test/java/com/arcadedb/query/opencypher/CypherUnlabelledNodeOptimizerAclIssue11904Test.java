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
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
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
 * A pattern with a node that has no label is planned by the optimizer from an index on a labelled node (issue #11904), where the
 * traditional plan walked the label (a range) or every vertex type (a pattern written from the unlabelled end). A walk opens the
 * buckets of the type, and a bucket refuses a user who may not read it whatever the bounds are. An index anchor loads a record only
 * when the index holds a key in the bounds, so left to itself it would refuse only then, and the statement would tell a user who may
 * not read the type whether it holds a key in a range. The refusal must be the same for bounds the denied type has keys in and for
 * bounds it has none in.
 * <p>
 * The planning of a user who may not read the type counts it and is refused before any anchor runs, but the cache of Cypher plans
 * belongs to the database, and a plan built for a user who may read the type is run as it is by one who may not: the anchors refuse
 * for themselves.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherUnlabelledNodeOptimizerAclIssue11904Test {
  private static final String PATH = "target/databases/CypherUnlabelledNodeOptimizerAclIssue11904Test";

  /** What a statement written with {@code $v} is run with: a bound that keys of the denied type are below, and one none is. */
  private static final int[] BOUNDS = { 100, -100 };

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
      for (final String name : new String[] { "Open", "Secret" }) {
        final VertexType type = database.getSchema().createVertexType(name);
        type.createProperty("id", Integer.class);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      }
      final EdgeType edge = database.getSchema().createEdgeType("K");
      edge.createProperty("w", Integer.class);
      edge.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "w");
    });
    database.transaction(() -> {
      final MutableVertex open = database.newVertex("Open").set("id", 1).save();
      for (int i = 1; i <= 5; i++) {
        final MutableVertex secret = database.newVertex("Secret").set("id", i).save();
        open.newEdge("K", secret, "w", i);
        secret.newEdge("K", open, "w", i + 10);
      }
    });
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  /**
   * The statements the optimizer plans from an index, each written so that {@code $v} decides what the index holds. None is a bare
   * {@code count(*)}, which the count push-down answers before any anchor runs. The last two are the same patterns fully labelled,
   * which the optimizer planned before issue #11904.
   */
  private static final String[] STATEMENTS = {
      "MATCH (p:Secret)-[:K]->(q) WHERE p.id < $v RETURN id(q) AS r",
      "MATCH (q)-[:K]->(p:Secret) WHERE p.id < $v RETURN id(q) AS r",
      "MATCH (p:Secret)-[:K]->(q) WHERE p.id >= $v AND p.id < 1000 RETURN id(q) AS r",
      "MATCH (p:Secret)-[:K]->(q) WHERE p.id = $v RETURN id(q) AS r",
      "MATCH (q)-[:K]->(p:Secret) WHERE p.id IN [$v, 1000] RETURN id(q) AS r",
      "MATCH (q)-[e:K]->(p:Secret) WHERE e.w = $v RETURN id(q) AS r",
      "MATCH (p:Secret)-[:K]->(q:Open) WHERE p.id < $v RETURN id(q) AS r",
      "MATCH (q:Open)-[:K]->(p:Secret) WHERE p.id = $v RETURN id(q) AS r" };

  /**
   * The cache of Cypher plans belongs to the database, and a plan built for a user who may read every type is run as it is by one
   * who may not. The refusal a walk makes up front, whatever the bounds are, is then the operator's to make.
   */
  @Test
  void aPlanCachedByAnotherUserRefusesAUserWhoMayNotReadTheTypeWhateverTheBoundsAre() {
    final List<String> notRefused = new ArrayList<>();
    for (final String statement : STATEMENTS) {
      deny();
      assertThat(explainOf(statement)).as(statement).contains("Cost-Based");
      drain(statement, 100);

      deny("Secret");
      for (final int bound : BOUNDS)
        if (!refused(statement, bound))
          notRefused.add(statement + " with $v = " + bound);
    }
    assertThat(notRefused).as("a denied type refuses the statement, it does not tell whether it holds a key").isEmpty();
  }

  private boolean refused(final String statement, final int bound) {
    return catchThrowable(() -> drain(statement, bound)) instanceof SecurityException;
  }

  @Test
  void aUserWhoMayReadEveryTypeIsAnswered() {
    deny();

    assertThat(drain("MATCH (p:Secret)-[:K]->(q) WHERE p.id < $v RETURN id(q) AS r", 100)).isEqualTo(5);
    assertThat(drain("MATCH (p:Secret)-[:K]->(q) WHERE p.id < $v RETURN id(q) AS r", -100)).isZero();
    assertThat(drain("MATCH (q)-[:K]->(p:Secret) WHERE p.id IN [$v, 3] RETURN id(q) AS r", -100)).isEqualTo(1);
    assertThat(drain("MATCH (q)-[e:K]->(p:Secret) WHERE e.w = $v RETURN id(q) AS r", 3)).isEqualTo(1);
  }

  private String explainOf(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher, Map.of("v", 100))) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  /** The number of rows of the statement. */
  private long drain(final String cypher, final int bound) {
    long rows = 0;
    try (final ResultSet rs = database.query("opencypher", cypher, Map.of("v", bound))) {
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
