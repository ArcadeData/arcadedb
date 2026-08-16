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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6090.
 * <p>
 * An ORPHAN EDGE RECORD is an edge record that exists physically in an edge-type bucket, whose {@code @out}/
 * {@code @in} name valid vertices, but which no vertex's edge list points back at: {@code countType()} counts it,
 * no traversal reaches it. {@code CHECK DATABASE} used to observe the condition - {@code checkEdges} probes both
 * endpoints for a back-reference - and then throw the finding away: both call sites did nothing but bump
 * {@code missingReferenceBack}, so no warning named the record, nothing entered {@code corruptedRecords} and
 * {@code FIX} never reclaimed it.
 * <p>
 * These tests pin the three things that changed: the probe is direction-aware (a healthy UNIDIRECTIONAL edge is
 * legitimately absent from its target's IN list and must not be reported), a genuine miss is warned and counted
 * under its own keys while {@code missingReferenceBack} keeps its old meaning, and reclaiming the records is an
 * explicit opt-in that plain {@code FIX} never performs.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6090OrphanEdgeRecordCheckTest extends TestHelper {

  private static final String VERTEX_TYPE       = "Issue6090Node";
  private static final String BIDIRECTIONAL_EDGE = "Issue6090BiLink";
  private static final String UNIDIRECTIONAL_EDGE = "Issue6090UniLink";

  /**
   * Every test here deliberately leaves an unreachable edge record behind - that is the state under measurement -
   * and the finding is now a warning, which is exactly what the shared teardown check refuses. Each test asserts
   * the check result itself, so the blanket teardown run would only re-assert the same database with the opposite
   * expectation.
   */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().buildEdgeType().withName(BIDIRECTIONAL_EDGE).withBidirectional(true).create();
      database.getSchema().buildEdgeType().withName(UNIDIRECTIONAL_EDGE).withBidirectional(false).create();
    });
  }

  /**
   * The headline case: a bidirectional edge record no list references at all. Before the fix the run reported a
   * bare {@code missingReferenceBack} of 2 and nothing else - no warning, no RID, no way to tell it from two
   * healthy unidirectional edges.
   */
  @Test
  void anOrphanBidirectionalEdgeRecordIsNamedAndCounted() {
    final RID orphan = createOrphanEdge(BIDIRECTIONAL_EDGE);

    final Result check = checkDatabase("CHECK DATABASE");

    assertThat(check.<Long>getProperty("unreachableEdgeRecords")).as("the orphan must be counted").isEqualTo(1L);
    assertThat(rids(check, "unreachableEdgeRecordsFound")).as("and named").containsExactly(orphan);
    assertThat(warnings(check)).as("and warned about, naming the record")
        .anyMatch(w -> w.contains(orphan.toString()) && w.contains("ORPHAN RECORD"));

    // Both sides are genuine defects for a bidirectional type.
    assertThat(check.<Long>getProperty("edgesMissingOutReference")).isEqualTo(1L);
    assertThat(check.<Long>getProperty("edgesMissingInReference")).isEqualTo(1L);

    // The pre-existing counter keeps its exact previous meaning: one bump per side that holds no reference.
    assertThat(check.<Long>getProperty("missingReferenceBack")).isEqualTo(2L);
  }

  /**
   * The reason the old counter could not be read as an orphan count: a perfectly healthy unidirectional edge is
   * legitimately absent from its target's IN list and bumps {@code missingReferenceBack} by 1. The new keys must
   * stay at zero for it, which is what makes them usable.
   */
  @Test
  void aHealthyUnidirectionalEdgeIsNotAFinding() {
    database.transaction(() -> {
      final MutableVertex source = database.newVertex(VERTEX_TYPE).set("name", "source").save();
      final MutableVertex target = database.newVertex(VERTEX_TYPE).set("name", "target").save();
      source.newEdge(UNIDIRECTIONAL_EDGE, target);
    });

    final Result check = checkDatabase("CHECK DATABASE");

    assertThat(check.<Long>getProperty("unreachableEdgeRecords")).isZero();
    assertThat(check.<Long>getProperty("edgesMissingOutReference")).isZero();
    assertThat(check.<Long>getProperty("edgesMissingInReference"))
        .as("a unidirectional edge is not expected in its target's IN list").isZero();
    assertThat(warnings(check)).isEmpty();

    // Unchanged, and this is the value that made the old counter unreadable.
    assertThat(check.<Long>getProperty("missingReferenceBack")).isEqualTo(1L);
  }

  /** The same detection must work when the type is unidirectional: only the OUT list can hold the reference. */
  @Test
  void anOrphanUnidirectionalEdgeRecordIsNamedAndCounted() {
    final RID orphan = createOrphanEdge(UNIDIRECTIONAL_EDGE);

    final Result check = checkDatabase("CHECK DATABASE");

    assertThat(check.<Long>getProperty("unreachableEdgeRecords")).isEqualTo(1L);
    assertThat(rids(check, "unreachableEdgeRecordsFound")).containsExactly(orphan);
    assertThat(check.<Long>getProperty("edgesMissingOutReference")).isEqualTo(1L);
    assertThat(check.<Long>getProperty("edgesMissingInReference"))
        .as("the IN side is not a reference holder for a unidirectional type, so it is not a defect").isZero();
  }

  /**
   * A HALF-linked edge - reachable from its source, missing only from its target's IN list - is a defect of the
   * incoming adjacency, not an orphan record. Reporting it as one would have {@code DELETE ORPHANS} destroy an
   * edge a traversal still reaches.
   */
  @Test
  void anEdgeStillReachableFromItsSourceIsNotAnOrphan() {
    final RID[] edge = new RID[1];
    final RID[] target = new RID[1];
    database.transaction(() -> {
      final MutableVertex source = database.newVertex(VERTEX_TYPE).set("name", "source").save();
      final MutableVertex destination = database.newVertex(VERTEX_TYPE).set("name", "target").save();
      edge[0] = source.newEdge(BIDIRECTIONAL_EDGE, destination).getIdentity();
      target[0] = destination.getIdentity();
    });

    // Drop ONLY the target's IN list, leaving the source's OUT list intact.
    database.transaction(() -> {
      final MutableVertex destination = target[0].asVertex(true).modify();
      destination.setInEdgesHeadChunk(null);
      destination.save();
    });

    final Result check = checkDatabase("CHECK DATABASE");

    assertThat(check.<Long>getProperty("unreachableEdgeRecords")).as("an OUT traversal still reaches it").isZero();
    assertThat(rids(check, "unreachableEdgeRecordsFound")).isEmpty();
    assertThat(check.<Long>getProperty("edgesMissingInReference")).isEqualTo(1L);
    assertThat(check.<Long>getProperty("edgesMissingOutReference")).isZero();
    assertThat(warnings(check)).anyMatch(w -> w.contains(edge[0].toString()) && w.contains("IN list"));
  }

  /**
   * Plain {@code FIX} must leave the record alone. Deleting a record nothing references is destructive - a vertex
   * whose head-chunk pointer was lost looks exactly like a source of orphans, and its edge records are the only
   * thing {@code RESTORE VERTEX} could rebuild the adjacency from - so it stays opt-in.
   */
  @Test
  void plainFixDoesNotDeleteAnOrphanEdgeRecord() {
    final RID orphan = createOrphanEdge(BIDIRECTIONAL_EDGE);

    final Result check = checkDatabase("CHECK DATABASE FIX");

    assertThat(check.<Long>getProperty("unreachableEdgeRecords")).as("still reported").isEqualTo(1L);
    assertThat(countType(BIDIRECTIONAL_EDGE)).as("but never removed by a plain FIX").isEqualTo(1L);
    assertThat(rids(check, "deletedRecordsAfterFix")).doesNotContain(orphan);
  }

  /** The explicit opt-in reclaims it, and the type stops over-reporting the record. */
  @Test
  void fixDeleteOrphansReclaimsTheOrphanEdgeRecord() {
    final RID orphan = createOrphanEdge(BIDIRECTIONAL_EDGE);

    final Result check = checkDatabase("CHECK DATABASE FIX DELETE ORPHANS");

    assertThat(check.<Long>getProperty("unreachableEdgeRecords")).isEqualTo(1L);
    assertThat(rids(check, "deletedRecordsAfterFix")).contains(orphan);
    assertThat(countType(BIDIRECTIONAL_EDGE)).as("the record is gone").isZero();

    // And a second run has nothing left to find.
    final Result second = checkDatabase("CHECK DATABASE");
    assertThat(second.<Long>getProperty("unreachableEdgeRecords")).isZero();
    assertThat(warnings(second)).isEmpty();
  }

  /** A healthy edge must survive the opt-in: the option reclaims orphans, not edges. */
  @Test
  void fixDeleteOrphansKeepsAReachableEdge() {
    database.transaction(() -> {
      final MutableVertex source = database.newVertex(VERTEX_TYPE).set("name", "source").save();
      final MutableVertex target = database.newVertex(VERTEX_TYPE).set("name", "target").save();
      source.newEdge(BIDIRECTIONAL_EDGE, target);
    });

    final Result check = checkDatabase("CHECK DATABASE FIX DELETE ORPHANS");

    assertThat(check.<Long>getProperty("unreachableEdgeRecords")).isZero();
    assertThat(countType(BIDIRECTIONAL_EDGE)).isEqualTo(1L);
  }

  /** {@code DELETE ORPHANS} is a repair, so it is meaningless - and refused - without {@code FIX}. */
  @Test
  void deleteOrphansWithoutFixIsRefused() {
    assertThatThrownBy(() -> database.command("sql", "CHECK DATABASE DELETE ORPHANS"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DELETE ORPHANS");
  }

  /**
   * The clause must survive alongside the rest of the statement, COMPRESS included - the grammar puts it between
   * {@code FIX} and {@code COMPRESS}, which is the one place an added optional clause can break a parse.
   * The grammar and {@code toString} round trip are pinned separately by
   * {@code Issue6090CheckDatabaseDeleteOrphansParserTest}.
   */
  @Test
  void theClauseCombinesWithTheRestOfTheStatement() {
    final RID orphan = createOrphanEdge(BIDIRECTIONAL_EDGE);

    final Result check = checkDatabase("CHECK DATABASE FIX DELETE ORPHANS COMPRESS");

    assertThat(check.<String>getProperty("operation")).isEqualTo("check database");
    assertThat(rids(check, "deletedRecordsAfterFix")).contains(orphan);
  }

  /**
   * Builds one edge record that no vertex references: a lone edge between two otherwise edge-less vertices, whose
   * endpoints then lose their head-chunk pointers. The record keeps valid {@code @out}/{@code @in} links, which is
   * what distinguishes an orphan from the dangling-link corruption the checker already reported.
   */
  private RID createOrphanEdge(final String edgeType) {
    final RID[] edge = new RID[1];
    final RID[] source = new RID[1];
    final RID[] target = new RID[1];

    database.transaction(() -> {
      final MutableVertex from = database.newVertex(VERTEX_TYPE).set("name", "source").save();
      final MutableVertex to = database.newVertex(VERTEX_TYPE).set("name", "target").save();
      edge[0] = from.newEdge(edgeType, to).getIdentity();
      source[0] = from.getIdentity();
      target[0] = to.getIdentity();
    });

    database.transaction(() -> {
      final MutableVertex from = source[0].asVertex(true).modify();
      from.setOutEdgesHeadChunk(null);
      from.save();

      final MutableVertex to = target[0].asVertex(true).modify();
      to.setInEdgesHeadChunk(null);
      to.save();
    });

    return edge[0];
  }

  private Result checkDatabase(final String command) {
    try (final ResultSet rs = database.command("sql", command)) {
      return rs.next();
    }
  }

  @SuppressWarnings("unchecked")
  private Collection<String> warnings(final Result check) {
    return (Collection<String>) check.getProperty("warnings");
  }

  @SuppressWarnings("unchecked")
  private Collection<RID> rids(final Result check, final String key) {
    return (Collection<RID>) check.getProperty(key);
  }

  private long countType(final String typeName) {
    final long[] count = new long[1];
    database.transaction(() -> count[0] = database.countType(typeName, true));
    return count[0];
  }
}
