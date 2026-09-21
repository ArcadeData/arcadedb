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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #8102, the same defect #7427 fixed on the {@code refactor.*} pair: ArcadeDB
 * declared a trailing argument mandatory that APOC declares with a default, so a call APOC accepts was rejected
 * with {@code CommandSemanticException}.
 * <p>
 * Two procedures were affected, and the argument that is actually optional differs between them - each is the
 * one APOC's own declaration gives a {@code defaultValue}:
 * <ul>
 *   <li>{@code apoc.merge.node(labels :: LIST<STRING>, identProps :: MAP, onCreateProps = {} :: MAP,
 *       onMatchProps = {} :: MAP)} - so {@code merge.node} has to accept two arguments. (ArcadeDB implements the
 *       first three; {@code onMatchProps} is issue #8117 and is out of scope here.)</li>
 *   <li>{@code apoc.do.when(condition :: BOOLEAN, ifQuery :: STRING, elseQuery :: STRING, params = {} :: MAP)} -
 *       so {@code do.when} has to accept three. {@code elseQuery} carries no default in APOC, which is why the
 *       two-argument call stays an error here, as {@code DoWhenTest.wrongArgumentCountThrows} already asserts.</li>
 * </ul>
 * The 3-argument {@code do.when} call also drives {@code DoWhen.isWriteProcedure(Object[])}, the parse-time
 * write-classification path of issue #6094, with a literal-argument array shorter than {@code getMaxArgs()}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherApocOptionalTrailingArgumentsIssue8102Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-apoc-optional-trailing-args-8102").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  // ---------------------------------------------------------------------------------------------------------
  // merge.node - onCreateProps omitted
  // ---------------------------------------------------------------------------------------------------------

  /** The call from the issue: APOC's own documentation example is this shape, {@code apoc.merge.node($labels, $identityProperties)}. */
  @Test
  void mergeNodeAcceptsTheCallWithoutOnCreateProps() {
    try (final ResultSet rs = database.command("opencypher",
        "CALL apoc.merge.node(['Person'], {name:'John'}) YIELD node RETURN node.name AS name")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("John");
    }

    assertThat(countOfPersons()).isEqualTo(1);
  }

  /** Omitting the argument must mean the empty map, not "skip the match": a second call merges onto the first node. */
  @Test
  void mergeNodeWithoutOnCreatePropsStillMergesOntoTheExistingNode() {
    mergeJohnWithoutCreateProps();
    mergeJohnWithoutCreateProps();

    assertThat(countOfPersons()).isEqualTo(1);
  }

  /** The three-argument form is unchanged - the defaulted slot is additive. */
  @Test
  void mergeNodeStillAcceptsOnCreateProps() {
    try (final ResultSet rs = database.command("opencypher",
        "CALL apoc.merge.node(['Person'], {name:'John'}, {age:30}) YIELD node RETURN node.age AS age")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("age").intValue()).isEqualTo(30);
    }
  }

  /** The arity gate moved, it did not disappear: one argument is still too few and four still too many. */
  @Test
  void mergeNodeStillRejectsAnArgumentCountOutsideItsNewBounds() {
    assertThatThrownBy(() -> database.command("opencypher",
        "CALL apoc.merge.node(['Person']) YIELD node RETURN node").hasNext())
        .isInstanceOf(CommandSemanticException.class);

    assertThatThrownBy(() -> database.command("opencypher",
        "CALL apoc.merge.node(['Person'], {name:'John'}, {age:30}, {seen:1}) YIELD node RETURN node").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  /**
   * The shortened call is still classified as a write. {@code Database.query()}'s idempotency gate and the HA
   * forward-to-leader decision both read this (issue #6094), so a call shape that became legal must not have
   * become invisible to them.
   */
  @Test
  void theShortenedMergeNodeCallIsStillClassifiedAsAWrite() {
    assertThat(isIdempotent("CALL apoc.merge.node(['Person'], {name:'John'}) YIELD node RETURN node")).isFalse();
  }

  // ---------------------------------------------------------------------------------------------------------
  // do.when - params omitted
  // ---------------------------------------------------------------------------------------------------------

  /** APOC defaults {@code params} to the empty map, so a sub-query that binds nothing needs no fourth argument. */
  @Test
  void doWhenAcceptsTheCallWithoutParams() {
    try (final ResultSet rs = database.command("opencypher",
        "CALL apoc.do.when(true, 'RETURN 1 AS x', '') YIELD value RETURN value.x AS x")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("x").intValue()).isEqualTo(1);
    }
  }

  /** The else branch of a three-argument call behaves exactly as it does with an explicit empty params map. */
  @Test
  void doWhenWithoutParamsRunsTheElseBranch() {
    try (final ResultSet rs = database.command("opencypher",
        "CALL apoc.do.when(false, 'RETURN 1 AS x', 'RETURN 2 AS x') YIELD value RETURN value.x AS x")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("x").intValue()).isEqualTo(2);
    }
  }

  /** A blank else branch on a false condition yields zero rows, with or without the params argument. */
  @Test
  void doWhenWithoutParamsAndABlankElseBranchYieldsNoRows() {
    try (final ResultSet rs = database.command("opencypher",
        "CALL apoc.do.when(false, 'RETURN 1 AS x', '') YIELD value RETURN value.x AS x")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** The four-argument form is unchanged. */
  @Test
  void doWhenStillAcceptsParams() {
    try (final ResultSet rs = database.command("opencypher",
        "CALL apoc.do.when(true, 'RETURN $n AS x', '', {n: 7}) YIELD value RETURN value.x AS x")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("x").intValue()).isEqualTo(7);
    }
  }

  /**
   * Two arguments stay an error. APOC declares {@code elseQuery :: STRING} with no default - only {@code params}
   * carries one - so the two-argument call is rejected by APOC too, and lowering the bound further would make
   * ArcadeDB accept a query Neo4j does not.
   */
  @Test
  void doWhenStillRejectsAnArgumentCountOutsideItsNewBounds() {
    assertThatThrownBy(() -> database.command("opencypher",
        "CALL apoc.do.when(true, 'RETURN 1 AS x') YIELD value RETURN value").hasNext())
        .isInstanceOf(CommandSemanticException.class);

    assertThatThrownBy(() -> database.command("opencypher",
        "CALL apoc.do.when(true, 'RETURN 1 AS x', '', {}, {}) YIELD value RETURN value").hasNext())
        .isInstanceOf(CommandSemanticException.class);
  }

  // ---------------------------------------------------------------------------------------------------------
  // do.when write classification on a call shorter than getMaxArgs()
  // ---------------------------------------------------------------------------------------------------------

  /**
   * {@code SimpleCypherStatement} hands {@code DoWhen.isWriteProcedure(Object[])} one entry per argument
   * <em>written at the call site</em>, so a three-argument call produces a length-3 array where the method used
   * to be able to assume four. The classification still has to weigh both branches - and reach them without
   * running off the end of the array.
   */
  @Test
  void aThreeArgumentDoWhenWithAWritingBranchIsClassifiedAsAWrite() {
    assertThat(isIdempotent("CALL apoc.do.when(true, 'CREATE (n:Person) RETURN n', '') YIELD value RETURN value"))
        .isFalse();
    assertThat(isIdempotent("CALL apoc.do.when(true, 'RETURN 1 AS x', 'CREATE (n:Person) RETURN n') YIELD value RETURN value"))
        .isFalse();
  }

  /** ...and a three-argument call whose two branches both only read is narrowed to read-only, as the four-argument one is. */
  @Test
  void aThreeArgumentDoWhenWithTwoReadingBranchesIsClassifiedAsReadOnly() {
    assertThat(isIdempotent("CALL apoc.do.when(true, 'RETURN 1 AS x', 'RETURN 2 AS x') YIELD value RETURN value"))
        .isTrue();
  }

  /** A write in a three-argument call really does persist, so the classification above is not academic. */
  @Test
  void aThreeArgumentDoWhenRunsItsWritingBranch() {
    try (final ResultSet rs = database.command("opencypher",
        "CALL apoc.do.when(true, \"CREATE (n:Person {name: 'Bob'}) RETURN n\", '') YIELD value RETURN value")) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
    }

    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person {name: 'Bob'}) RETURN p")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  // ---------------------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------------------

  private void mergeJohnWithoutCreateProps() {
    try (final ResultSet rs = database.command("opencypher",
        "CALL apoc.merge.node(['Person'], {name:'John'}) YIELD node RETURN node")) {
      assertThat(rs.hasNext()).isTrue();
      rs.next();
    }
  }

  private long countOfPersons() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) RETURN count(p) AS total")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      return ((Number) row.getProperty("total")).longValue();
    }
  }

  private boolean isIdempotent(final String query) {
    return database.getQueryEngine("opencypher").analyze(query).isIdempotent();
  }
}
