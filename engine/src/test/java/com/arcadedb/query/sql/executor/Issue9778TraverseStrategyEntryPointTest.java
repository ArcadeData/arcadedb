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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9778: the two TRAVERSE strategies differ in the order they visit nodes and in nothing else, so a row that is a
 * valid entry point for one is a valid entry point for the other. The breadth-first step turned a one-column row holding a
 * bare RID ({@code SELECT @rid ...}, or a LINK property) into no entry point at all and answered an empty result, while the
 * depth-first step loaded the record and walked the whole graph.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9778TraverseStrategyEntryPointTest extends TestHelper {
  private static final String[] SOURCES = {//
      "[%s]",//
      "(SELECT @this AS v FROM V WHERE name = 'a')",//
      "(SELECT FROM V WHERE name = 'a')",//
      "(SELECT @rid FROM V WHERE name = 'a')",//
      "(SELECT @rid AS r FROM V WHERE name = 'a')",//
      "(SELECT link FROM L)",//
      "(SELECT expand(link) FROM L)" };

  private String aRid;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E");
    database.getSchema().createDocumentType("L").createProperty("link", Type.LINK);

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").set("name", "a").save();
      final MutableVertex b = database.newVertex("V").set("name", "b").save();
      final MutableVertex c = database.newVertex("V").set("name", "c").save();
      a.newEdge("E", b);
      b.newEdge("E", c);
      database.newDocument("L").set("link", a.getIdentity()).save();
      aRid = a.getIdentity().toString();
    });
  }

  @Test
  void everyEntryPointShapeTraversesTheWholeGraphUnderBothStrategies() {
    for (final String source : SOURCES) {
      final String from = source.formatted(aRid);
      assertThat(traverse(from, "DEPTH_FIRST")).as("DEPTH_FIRST from %s", from).containsExactlyInAnyOrder("a", "b", "c");
      assertThat(traverse(from, "BREADTH_FIRST")).as("BREADTH_FIRST from %s", from).containsExactlyInAnyOrder("a", "b", "c");
    }
  }

  /**
   * A row of two or more columns is not an entry point under either strategy. Pinned here for what the issue asks of every
   * shape: the two strategies give the same answer.
   */
  @Test
  void aMultiColumnRowGivesTheSameAnswerUnderBothStrategies() {
    final String from = "(SELECT @rid AS r, name FROM V WHERE name = 'a')";
    assertThat(traverse(from, "BREADTH_FIRST")).isEqualTo(traverse(from, "DEPTH_FIRST"));
  }

  /** A LINK left null projects a one-column row with nothing to start from: skipped by both strategies, never an error. */
  @Test
  void aNullLinkIsSkippedByBothStrategies() {
    database.transaction(() -> database.newDocument("L").save());

    for (final String strategy : new String[] { "DEPTH_FIRST", "BREADTH_FIRST" })
      assertThat(traverse("(SELECT link FROM L)", strategy)).as(strategy).containsExactlyInAnyOrder("a", "b", "c");
  }

  /** A LINK whose record was deleted has nothing to start from either: skipped like a null one, not a failed query. */
  @Test
  void aLinkToADeletedRecordIsSkippedByBothStrategies() {
    database.transaction(() -> {
      final MutableVertex gone = database.newVertex("V").set("name", "gone").save();
      database.newDocument("L").set("link", gone.getIdentity()).save();
      gone.delete();
    });

    for (final String strategy : new String[] { "DEPTH_FIRST", "BREADTH_FIRST" })
      assertThat(traverse("(SELECT link FROM L)", strategy)).as(strategy).containsExactlyInAnyOrder("a", "b", "c");
  }

  private Set<String> traverse(final String from, final String strategy) {
    final Set<String> names = new HashSet<>();
    try (final ResultSet rs = database.query("sql", "TRAVERSE out() FROM " + from + " STRATEGY " + strategy)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }
}
