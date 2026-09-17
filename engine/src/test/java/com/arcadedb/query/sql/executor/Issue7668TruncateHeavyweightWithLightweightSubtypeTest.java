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
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7668: {@code TRUNCATE TYPE} on a heavyweight edge type that merely has a LIGHTWEIGHT
 * subtype used to route through {@code EdgeType.holdsLightweightEdges()} to the batched, index-unaware
 * {@code DELETE FROM} path meant only for a genuinely lightweight type - re-introducing the #4352 tombstone hazard
 * on the heavyweight type's own index, and never actually reaching the lightweight subtype's edges (which have no
 * bucket for a record-based delete to find).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7668TruncateHeavyweightWithLightweightSubtypeTest extends TestHelper {

  @Test
  void truncatePolymorphicClearsHeavyweightIndexAndLightweightSubtypeEdges() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Follows");
    database.command("sql", "CREATE PROPERTY Follows.since STRING");
    database.command("sql", "CREATE INDEX ON Follows(since) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE CloseFollows EXTENDS Follows LIGHTWEIGHT");

    final RID p1, p2, p3, p4;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      p3 = database.newVertex("Person").set("name", "p3").save().getIdentity();
      p4 = database.newVertex("Person").set("name", "p4").save().getIdentity();

      database.lookupByRID(p1, true).asVertex().modify().newEdge("Follows", p2).set("since", "2020").save();
      database.lookupByRID(p3, true).asVertex().modify().newEdge("CloseFollows", p4);
    } finally {
      database.commit();
    }

    assertCount("Follows", 2);
    assertIndexSize("Follows[since]", 1);
    assertThat(database.lookupByRID(p3, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "CloseFollows"))
        .isEqualTo(1);

    // Fingerprint the index's underlying LSM-Tree files: the safe path (drop the index, clear the records, rebuild
    // an empty one) replaces them, while the buggy path - a bucket-scan delete with the index left live - keeps
    // deleting into the very same files. This is what actually distinguishes the two paths: at this small scale
    // both leave the index correctly sized, so only the file identity tells them apart (issue #7668).
    final List<Integer> indexFileIdsBeforeTruncate = List.copyOf(((TypeIndex) database.getSchema().getIndexByName("Follows[since]")).getFileIds());

    // outside a transaction: this is the fast, own-transaction path that used to be skipped for the whole tree
    // whenever ANY type in it was lightweight (issue #7668)
    database.command("sql", "TRUNCATE TYPE Follows POLYMORPHIC UNSAFE");

    assertCount("Follows", 0);
    // the index must be empty, not just unreachable through a bucket scan - reusing the same key must succeed
    assertIndexSize("Follows[since]", 0);
    assertThat(((TypeIndex) database.getSchema().getIndexByName("Follows[since]")).getFileIds())
        .as("Follows has its own index and properties: it must take the index-drop/rebuild path, not the raw "
            + "batched delete meant only for a type with no index to protect (issue #7668)")
        .isNotEqualTo(indexFileIdsBeforeTruncate);
    assertThat(database.lookupByRID(p3, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "CloseFollows"))
        .as("the lightweight subtype's edge has no bucket of its own: only the walk-based DELETE FROM reaches it")
        .isEqualTo(0);

    // the recreated index must accept the same key again (used to fail with a stale-tombstone duplicate-key error,
    // issue #4352, if the heavyweight part were ever routed through the raw batched delete instead)
    database.begin();
    try {
      database.lookupByRID(p1, true).asVertex().modify().newEdge("Follows", p2).set("since", "2020").save();
    } finally {
      database.commit();
    }
    assertCount("Follows", 1);
    assertIndexSize("Follows[since]", 1);
  }

  /**
   * Symmetric shape: the LIGHTWEIGHT type is the root and the heavyweight, indexed type is the subtype. A type's
   * own LIGHTWEIGHT flag is independent of its parent's, so this hierarchy is just as legal as the one above and
   * exercises the same routing decision from the other side.
   */
  @Test
  void truncatePolymorphicClearsLightweightRootAndHeavyweightSubtypeIndex() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Knows LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE StrongKnows EXTENDS Knows");
    database.command("sql", "CREATE PROPERTY StrongKnows.since STRING");
    database.command("sql", "CREATE INDEX ON StrongKnows(since) UNIQUE");

    final RID p1, p2, p3, p4;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      p3 = database.newVertex("Person").set("name", "p3").save().getIdentity();
      p4 = database.newVertex("Person").set("name", "p4").save().getIdentity();

      database.lookupByRID(p1, true).asVertex().modify().newEdge("Knows", p2);
      database.lookupByRID(p3, true).asVertex().modify().newEdge("StrongKnows", p4).set("since", "2020").save();
    } finally {
      database.commit();
    }

    assertCount("Knows", 2);
    assertIndexSize("StrongKnows[since]", 1);

    final List<Integer> indexFileIdsBeforeTruncate = List.copyOf(((TypeIndex) database.getSchema().getIndexByName("StrongKnows[since]")).getFileIds());

    database.command("sql", "TRUNCATE TYPE Knows POLYMORPHIC UNSAFE");

    assertCount("Knows", 0);
    assertIndexSize("StrongKnows[since]", 0);
    assertThat(((TypeIndex) database.getSchema().getIndexByName("StrongKnows[since]")).getFileIds())
        .as("StrongKnows has its own index and properties: it must take the index-drop/rebuild path even though "
            + "its LIGHTWEIGHT parent Knows has none (issue #7668)")
        .isNotEqualTo(indexFileIdsBeforeTruncate);
    assertThat(database.lookupByRID(p1, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "Knows"))
        .isEqualTo(0);

    database.begin();
    try {
      database.lookupByRID(p3, true).asVertex().modify().newEdge("StrongKnows", p4).set("since", "2020").save();
    } finally {
      database.commit();
    }
    assertCount("StrongKnows", 1);
    assertIndexSize("StrongKnows[since]", 1);
  }

  /**
   * A LIGHTWEIGHT type sandwiched between two heavyweight, indexed ones (CodeRabbit review on this PR): {@code
   * getSubTypes()} returns direct subtypes only, so an index-collection walk that stops at depth one would miss
   * VeryCloseFollows' index entirely while the record-backed scan beneath it - which walks the full subtype tree
   * regardless of depth - deletes its records anyway, reintroducing the #4352 tombstone hazard for exactly the
   * type this fix exists to protect, two levels down instead of one.
   */
  @Test
  void truncatePolymorphicClearsAnIndexTwoLevelsBelowALightweightIntermediateType() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Follows");
    database.command("sql", "CREATE EDGE TYPE CloseFollows EXTENDS Follows LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE VeryCloseFollows EXTENDS CloseFollows");
    database.command("sql", "CREATE PROPERTY VeryCloseFollows.since STRING");
    database.command("sql", "CREATE INDEX ON VeryCloseFollows(since) UNIQUE");

    final RID p1, p2;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      database.lookupByRID(p1, true).asVertex().modify().newEdge("VeryCloseFollows", p2).set("since", "2020").save();
    } finally {
      database.commit();
    }

    assertCount("VeryCloseFollows", 1);
    assertIndexSize("VeryCloseFollows[since]", 1);

    final List<Integer> indexFileIdsBeforeTruncate =
        List.copyOf(((TypeIndex) database.getSchema().getIndexByName("VeryCloseFollows[since]")).getFileIds());

    database.command("sql", "TRUNCATE TYPE Follows POLYMORPHIC UNSAFE");

    assertCount("VeryCloseFollows", 0);
    assertIndexSize("VeryCloseFollows[since]", 0);
    assertThat(((TypeIndex) database.getSchema().getIndexByName("VeryCloseFollows[since]")).getFileIds())
        .as("VeryCloseFollows' index is two levels below the LIGHTWEIGHT type collectTruncationScope routed "
            + "through: collectIndexDefinitions must recurse the same full depth to find and protect it")
        .isNotEqualTo(indexFileIdsBeforeTruncate);

    database.begin();
    try {
      database.lookupByRID(p1, true).asVertex().modify().newEdge("VeryCloseFollows", p2).set("since", "2020").save();
    } finally {
      database.commit();
    }
    assertCount("VeryCloseFollows", 1);
    assertIndexSize("VeryCloseFollows[since]", 1);
  }

  private void assertCount(final String typeName, final long expected) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) as cnt FROM " + typeName)) {
      assertThat(rs.next().<Long>getProperty("cnt")).isEqualTo(expected);
    }
  }

  private void assertIndexSize(final String indexName, final long expected) {
    final RangeIndex index = (RangeIndex) database.getSchema().getIndexByName(indexName);
    long count = 0;
    final IndexCursor cursor = index.iterator(true);
    while (cursor.hasNext()) {
      cursor.next();
      count++;
    }
    assertThat(count).as("Index '%s' size", indexName).isEqualTo(expected);
  }
}
