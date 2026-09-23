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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #8072, a follow-up to #7858: the lookup query {@code merge.node} builds to
 * decide whether a node already exists spliced the caller's label and every match-property key between raw
 * back-ticks.
 * <p>
 * Inside a back-tick quoted identifier a backslash escapes the character after it, so {@code `a\b`} names the
 * property {@code ab} and {@code `a\`} never closes at all. The procedure therefore looked up a name the caller
 * never asked for - matching nothing, so a second node was created - or produced a statement that does not parse.
 * Both splices now go through {@link com.arcadedb.query.sql.parser.Identifier#quote}, the helper #7858
 * standardised on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMergeNodeIdentifierQuotingIssue8072Test {
  private static final String MERGE = "CALL merge.node($labels, $match, $create) YIELD node RETURN node";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-merge-node-identifier-quoting-8072").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /**
   * A backslash inside the label used to be swallowed by the SQL parser, so the lookup ran against a type that
   * does not exist and every call merged a fresh node.
   */
  @Test
  void aLabelCarryingABackslashMergesOntoTheSameNode() {
    mergeTwice(List.of("Pers\\on"), Map.of("id", 1));
    assertThat(countOf("Pers\\on")).isEqualTo(1);
  }

  /** The same for a back-tick, which closed the identifier early and ran the rest of the name as SQL. */
  @Test
  void aLabelCarryingABackTickMergesOntoTheSameNode() {
    mergeTwice(List.of("Pers`on"), Map.of("id", 1));
    assertThat(countOf("Pers`on")).isEqualTo(1);
  }

  /** A trailing backslash escapes the closing back-tick, so the statement did not even parse. */
  @Test
  void aLabelEndingWithABackslashMergesOntoTheSameNode() {
    mergeTwice(List.of("Person\\"), Map.of("id", 1));
    assertThat(countOf("Person\\")).isEqualTo(1);
  }

  /**
   * The property keys of {@code matchProps} are the second splice, and a name mangled there makes the WHERE
   * clause compare a property nobody wrote - which matches nothing and merges a duplicate.
   */
  @Test
  void aMatchPropertyKeyCarryingABackslashMergesOntoTheSameNode() {
    mergeTwice(List.of("Backslash8072"), Map.of("we\\ird", 1));
    assertThat(countOf("Backslash8072")).isEqualTo(1);
  }

  /** Same splice, back-tick instead: it closed the identifier and left {@code on` = ?} as loose SQL. */
  @Test
  void aMatchPropertyKeyCarryingABackTickMergesOntoTheSameNode() {
    mergeTwice(List.of("BackTick8072"), Map.of("we`ird", 1));
    assertThat(countOf("BackTick8072")).isEqualTo(1);
  }

  /** A key ending with a backslash swallowed the closing back-tick and ran {@code = ?} into the identifier. */
  @Test
  void aMatchPropertyKeyEndingWithABackslashMergesOntoTheSameNode() {
    mergeTwice(List.of("TrailingSlash8072"), Map.of("weird\\", 1));
    assertThat(countOf("TrailingSlash8072")).isEqualTo(1);
  }

  /**
   * The node the second call returns has to be the one the first created, not merely a node with the same
   * count: a lookup that quietly matches the wrong record would satisfy the counts above.
   */
  @Test
  void theSecondCallReturnsTheNodeTheFirstCreated() {
    final String firstRid = mergeOnce(List.of("Ident\\ity8072"), Map.of("we\\ird", 7));
    final String secondRid = mergeOnce(List.of("Ident\\ity8072"), Map.of("we\\ird", 7));
    assertThat(secondRid).isEqualTo(firstRid);
  }

  /**
   * The escaping must not make two genuinely different names collide: a property key that the old splice
   * mangled INTO another key has to stay distinct from it.
   */
  @Test
  void twoKeysThatTheOldSpliceCollapsedStayDistinct() {
    mergeOnce(List.of("Distinct8072"), Map.of("we\\ird", 1));
    mergeOnce(List.of("Distinct8072"), Map.of("weird", 1));
    assertThat(countOf("Distinct8072")).as("`we\\ird` and `weird` are different properties").isEqualTo(2);
  }

  private void mergeTwice(final List<String> labels, final Map<String, Object> matchProps) {
    mergeOnce(labels, matchProps);
    mergeOnce(labels, matchProps);
  }

  private String mergeOnce(final List<String> labels, final Map<String, Object> matchProps) {
    try (final ResultSet resultSet = database.command("opencypher", MERGE,
        Map.of("labels", labels, "match", matchProps, "create", Map.of()))) {
      assertThat(resultSet.hasNext()).isTrue();
      final Vertex node = resultSet.next().getProperty("node");
      return node.getIdentity().toString();
    }
  }

  private long countOf(final String typeName) {
    return database.countType(typeName, false);
  }
}
