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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code CHECK DATABASE RECORD <rid>}: the check narrowed to named records instead of whole types.
 * <p>
 * Motivated by #5680. Deleting a vertex is now strict about an edge list it cannot walk, so a genuinely broken
 * chain makes the vertex undeletable until {@code CHECK DATABASE ... FIX} rebuilds its adjacency from the
 * surviving edge records. That repair could only be aimed at a TYPE or a BUCKET, so recovering one vertex meant
 * two full passes over its entire vertex type. The RECORD scope does the identical per-record work and the
 * identical rebuild, over just the RIDs named.
 * <p>
 * One cost it deliberately does NOT bound: rebuilding an adjacency means finding every surviving edge that points
 * at the vertex, and no index maps endpoints back to edges, so the scoped run saves the vertex passes and still
 * scans the edge types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseRecordScopeTest extends TestHelper {

  /** These tests deliberately break an edge-list chain, so the blanket end-of-test check would always fire. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  /**
   * The repair, end to end: a vertex whose head chunk is gone cannot be deleted, the scoped check rebuilds its
   * adjacency from the surviving edge records, and the ordinary delete then completes and takes every edge with
   * it - the outcome a tolerant delete could never produce, since it would only convert the broken chain into
   * edges that outlive their vertex.
   */
  @Test
  void checkDatabaseRecordFixMakesTheVertexDeletableAgain() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 200);

    deleteRecord(inChunkChain(hubRID).get(0));

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE RECORD " + hubRID + " FIX")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      // The scoped run must have found the broken chain and rebuilt it - not reported a clean record.
      assertThat((Collection<String>) row.getProperty("warnings")).as("%s", row.toJSON())
          .anyMatch(w -> w.contains(hubRID.toString()) && w.contains("rebuilding the edge list"))
          .anyMatch(w -> w.contains("reconnected " + edges.size() + " incoming edges"));
    }

    database.transaction(
        () -> assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(edges.size()));

    database.transaction(() -> hubRID.asVertex().delete(), false, 1);

    database.transaction(() -> {
      assertThat(database.existsRecord(hubRID)).isFalse();
      for (final RID e : edges)
        assertThat(database.existsRecord(e)).as("edge " + e + " outlived its vertex").isFalse();
    });

    assertIntegrityClean();
  }

  /**
   * RECORD must really be a scope: a check naming a healthy vertex must not report - nor repair - the broken chain
   * of a different vertex of the same type. Without this, a "scoped" run that quietly fell back to a type-wide
   * scan would still satisfy every other assertion here.
   */
  @Test
  void checkDatabaseRecordVisitsOnlyTheNamedRecord() {
    createSchema();
    final RID brokenHub = createHub();
    createEdges(brokenHub, 200);
    final RID healthyHub = createHub();
    createEdges(healthyHub, 20);

    deleteRecord(inChunkChain(brokenHub).get(0));

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE RECORD " + healthyHub + " FIX")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Collection<String>) row.getProperty("warnings")).as("%s", row.toJSON()).isEmpty();
    }

    // Untouched: the broken hub is still broken, so the scoped run genuinely never looked at it.
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE RECORD " + brokenHub)) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Collection<String>) row.getProperty("warnings")).as("%s", row.toJSON())
          .anyMatch(w -> w.contains(brokenHub.toString()));
    }
  }

  /**
   * RECORD is already the narrowest scope, so combining it with TYPE or BUCKET can only mean the caller expected
   * something the command does not do. Letting RECORD silently win would run a check nobody asked for, and an
   * intersection would be a third semantics nobody asked for either.
   */
  @Test
  void checkDatabaseRecordRejectsBeingCombinedWithTypeOrBucket() {
    createSchema();
    final RID hubRID = createHub();
    createEdges(hubRID, 5);

    assertThatThrownBy(() -> database.command("sql", "CHECK DATABASE TYPE Hub RECORD " + hubRID + " FIX"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be combined with TYPE or BUCKET");

    assertThatThrownBy(() -> database.command("sql", "CHECK DATABASE BUCKET Hub RECORD " + hubRID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be combined with TYPE or BUCKET");

    // Each on its own still works.
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE RECORD " + hubRID)) {
      assertThat(rs.hasNext()).isTrue();
    }
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE TYPE Hub")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }

  /**
   * A RID that simply is not there is reported but NOT flagged corrupted, and that distinction has teeth: a
   * corrupted record puts its bucket into the affected set, and {@code FIX} then drops and rebuilds every index on
   * it - a full bucket scan, exactly the cost the RECORD scope exists to avoid. Since the scope is meant to be
   * hand-typed after a failed delete, a stale or mistyped RID must not buy that.
   */
  @Test
  void checkDatabaseRecordDoesNotTreatAMissingRidAsCorruption() {
    createSchema();
    final RID hubRID = createHub();
    createEdges(hubRID, 5);

    // A valid RID in a real bucket whose record is gone.
    final RID[] goneHolder = new RID[1];
    database.transaction(() -> {
      final MutableVertex doomed = database.newVertex("Src");
      doomed.save();
      goneHolder[0] = doomed.getIdentity();
    });
    database.transaction(() -> goneHolder[0].asVertex().delete());

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE RECORD " + goneHolder[0] + " FIX")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Collection<String>) row.getProperty("warnings")).as("%s", row.toJSON())
          .anyMatch(w -> w.contains(goneHolder[0].toString()) && w.contains("does not exist"));
      assertThat(longProperty(row, "totalCorruptedRecords")).as("a missing RID is not corruption: %s", row.toJSON())
          .isEqualTo(0L);
      // The give-away that it was not flagged: nothing was repaired, so no index on its bucket was rebuilt.
      assertThat(longProperty(row, "autoFix")).as("%s", row.toJSON()).isEqualTo(0L);
      assertThat((Collection<String>) row.getProperty("rebuiltIndexes")).as("%s", row.toJSON()).isEmpty();
    }
  }

  /** An EDGE-typed RID takes the edge arm of the scope, checking its endpoints rather than an adjacency. */
  @Test
  void checkDatabaseRecordAcceptsAnEdgeRid() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 5);

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE RECORD " + edges.get(0))) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Collection<String>) row.getProperty("warnings")).as("a healthy edge: %s", row.toJSON()).isEmpty();
    }

    // With its IN endpoint gone, the same scoped check reports the edge's dangling link.
    database.transaction(() -> database.getSchema().getBucketById(hubRID.getBucketId()).deleteRecord(hubRID));

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE RECORD " + edges.get(0))) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(longProperty(row, "invalidLinks")).as("%s", row.toJSON()).isGreaterThan(0L);
    }
  }

  /** A DOCUMENT-typed RID takes the document arm, and its progress steps must stay within the budgeted total. */
  @Test
  void checkDatabaseRecordAcceptsADocumentRid() {
    createSchema();
    database.transaction(() -> database.getSchema().createDocumentType("Doc"));

    final RID[] docHolder = new RID[1];
    database.transaction(() -> docHolder[0] = database.newDocument("Doc").set("k", 1).save().getIdentity());

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE RECORD " + docHolder[0])) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Collection<String>) row.getProperty("warnings")).as("a healthy document: %s", row.toJSON())
          .isEmpty();
      assertThat(longProperty(row, "totalCorruptedRecords")).as("%s", row.toJSON()).isEqualTo(0L);
    }
  }

  /**
   * Several RIDs spanning different types in one command - the whole point of grouping by type. Each group must
   * reach the arm that matches it, and a broken vertex among them must still be repaired.
   */
  @Test
  void checkDatabaseRecordAcceptsRidsSpanningSeveralTypes() {
    createSchema();
    database.transaction(() -> database.getSchema().createDocumentType("Doc"));

    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 200);
    final RID[] docHolder = new RID[1];
    database.transaction(() -> docHolder[0] = database.newDocument("Doc").set("k", 1).save().getIdentity());

    deleteRecord(inChunkChain(hubRID).get(0));

    final String command =
        "CHECK DATABASE RECORD " + hubRID + ", " + edges.get(0) + ", " + docHolder[0] + " FIX";
    try (final ResultSet rs = database.command("sql", command)) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Collection<String>) row.getProperty("warnings")).as("%s", row.toJSON())
          .anyMatch(w -> w.contains(hubRID.toString()) && w.contains("rebuilding the edge list"));
    }

    // The vertex group really was repaired, not merely reported.
    database.transaction(
        () -> assertThat(hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK")).isEqualTo(edges.size()));
  }

  /** A RID whose bucket belongs to no type is reported rather than silently ignored. */
  @Test
  void checkDatabaseRecordReportsARidBelongingToNoType() {
    createSchema();
    createHub();

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE RECORD #9999:0")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Collection<String>) row.getProperty("warnings")).as("%s", row.toJSON())
          .anyMatch(w -> w.contains("#9999:0") && w.contains("does not belong to any type"));
    }
  }

  private void createSchema() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Src", 16);
      database.getSchema().createEdgeType("LINK", 16);
    });
  }

  private RID createHub() {
    final MutableVertex[] holder = new MutableVertex[1];
    database.transaction(() -> {
      holder[0] = database.newVertex("Hub");
      holder[0].save();
    });
    return holder[0].getIdentity();
  }

  /** One edge per transaction, so the hub's IN chain grows chunk by chunk exactly as it does in production. */
  private List<RID> createEdges(final RID hubRID, final int count) {
    final List<RID> edges = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final MutableVertex src = database.newVertex("Src");
        src.save();
        holder[0] = src.newEdge("LINK", hubRID).getIdentity();
      });
      edges.add(holder[0]);
    }
    return edges;
  }

  /** The hub's IN chunk chain, head first (newest chunk) to tail (the chunk created with the first edge). */
  private List<RID> inChunkChain(final RID hubRID) {
    final List<RID> chain = new ArrayList<>();
    database.transaction(() -> {
      RID rid = ((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk();
      while (rid != null) {
        chain.add(rid);
        rid = ((EdgeSegment) database.lookupByRID(rid, true)).getPreviousRID();
      }
    });
    return chain;
  }

  private void deleteRecord(final RID rid) {
    database.transaction(() -> database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid));
    database.transaction(() -> assertThat(database.existsRecord(rid)).isFalse());
  }

  /** Asserts on the fields {@code check database} actually reports, so a typo cannot make this vacuously pass. */
  private void assertIntegrityClean() {
    try (final ResultSet rs = database.command("sql", "check database")) {
      assertThat(rs.hasNext()).isTrue();
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(longProperty(row, "autoFix")).as("autoFix: %s", row.toJSON()).isEqualTo(0L);
        assertThat(longProperty(row, "invalidLinks")).as("invalidLinks: %s", row.toJSON()).isEqualTo(0L);
        assertThat(longProperty(row, "totalWarnings")).as("totalWarnings: %s", row.toJSON()).isEqualTo(0L);
        assertThat(longProperty(row, "totalCorruptedRecords")).as("totalCorruptedRecords: %s", row.toJSON())
            .isEqualTo(0L);
      }
    }
  }

  /** Reads a numeric check-database property, failing loudly when the field does not exist (a vacuous assertion). */
  private static long longProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    assertThat(value).as("check database must report '%s': %s", name, row.toJSON()).isNotNull();
    return ((Number) value).longValue();
  }
}
