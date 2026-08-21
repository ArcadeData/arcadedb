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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6488: {@code RETURN DISTINCT} (and {@code WITH DISTINCT}) must collapse
 * rows whose projected variables are equal, even when one of the two matching rows binds its variable
 * to a graph-element reference whose property buffer has not yet been deserialized.
 * <p>
 * On a single self-looping node, the quantified relationship {@code [*0..1]} enumerates two paths
 * that both bind {@code n0} and {@code n1} to the same node: the zero-length path (whose {@code n1}
 * reference is the already-loaded start vertex) and the one-edge self-loop path (whose {@code n1}
 * reference is freshly obtained from the edge's endpoint and not yet deserialized). Both references
 * carry the same RID, but {@code Document.toString()} renders a not-yet-loaded reference as a
 * placeholder (e.g. {@code #1:0[?]}) instead of the actual property values - the root cause was
 * DISTINCT building its deduplication key by rendering values with {@code toString()} instead of by
 * their stable identity. {@code RETURN DISTINCT n1, n0} (and {@code WITH DISTINCT n1, n0}) must
 * collapse the two rows to one - matching Neo4j, the openCypher reference implementation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6488DistinctHiddenPathVariableTest {
  private Database database;

  @BeforeEach
  public void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue6488");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      database.command("cypher", "CREATE (a:A {name: 'a'})");
      database.command("cypher", "MATCH (a:A {name: 'a'}) CREATE (a)-[:R]->(a)");
    });
  }

  @AfterEach
  public void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * The reported case: an unreturned path variable {@code p} must not defeat {@code DISTINCT}.
   */
  @Test
  public void distinctCollapsesSelfLoopZeroAndOneHopPaths() {
    try (ResultSet rs = database.query("cypher", """
        MATCH p = WALK (n0)-[*0..1]->(n1)
        RETURN DISTINCT n1, n0""")) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).as("DISTINCT must collapse the zero-length and self-loop paths into a single row").isFalse();
    }
  }

  /**
   * Same query, but with an identity {@code CALL (*)} boundary interposed between the MATCH and the
   * final RETURN. Both forms must agree.
   */
  @Test
  public void distinctCollapsesSelfLoopPathsAcrossIdentitySubquery() {
    try (ResultSet rs = database.query("cypher", """
        MATCH p = WALK (n0)-[*0..1]->(n1)
        WITH n0, n1
        CALL (*) {
          RETURN 0 AS marker
        }
        WITH n0, n1
        RETURN DISTINCT n1, n0""")) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * {@code WITH DISTINCT} hit the same root cause via a different code path: it built its
   * deduplication key from the projected row's {@code toString()} instead of canonicalizing each
   * value, so it was equally susceptible to the {@code Document.toString()} load-state instability.
   */
  @Test
  public void withDistinctCollapsesSelfLoopZeroAndOneHopPaths() {
    try (ResultSet rs = database.query("cypher", """
        MATCH p = WALK (n0)-[*0..1]->(n1)
        WITH DISTINCT n1, n0
        RETURN n1, n0""")) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).as("WITH DISTINCT must collapse the zero-length and self-loop paths into a single row").isFalse();
    }
  }

  /**
   * {@code UNION} (implicit DISTINCT) deduplicates through the very same
   * {@code DistinctNumericKey.canonicalize()} utility as {@code RETURN DISTINCT}. Union the
   * already-loaded reference from a plain match with the freshly-fetched, not-yet-deserialized
   * reference from the self-loop traversal, and confirm they still collapse to one row.
   */
  @Test
  public void unionCollapsesLoadedAndUnloadedReferencesToSameNode() {
    try (ResultSet rs = database.query("cypher", """
        MATCH p = WALK (n0)-[*0..1]->(n1)
        RETURN n1 AS x
        UNION
        MATCH (a:A)
        RETURN a AS x""")) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
      assertThat(rs.hasNext()).as("UNION must collapse references to the same node regardless of load state").isFalse();
    }
  }

  /**
   * {@code collect(DISTINCT ...)} and {@code count(DISTINCT ...)} go through
   * {@code DistinctAggregationWrapper}, which also canonicalizes via {@code DistinctNumericKey}.
   */
  @Test
  public void collectAndCountDistinctCollapseSelfLoopZeroAndOneHopPaths() {
    try (ResultSet rs = database.query("cypher", """
        MATCH p = WALK (n0)-[*0..1]->(n1)
        RETURN collect(DISTINCT n1) AS xs, count(DISTINCT n1) AS c""")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((List<?>) row.getProperty("xs")).hasSize(1);
      assertThat(row.<Number>getProperty("c").intValue()).isEqualTo(1);
    }
  }
}
