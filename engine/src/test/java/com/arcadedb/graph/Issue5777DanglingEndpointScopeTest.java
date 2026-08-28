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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5777: what {@code CHECK DATABASE} does with an edge endpoint that IS NOT THERE.
 * <p>
 * Two defects, one cause. {@code GraphDatabaseChecker} carried FOUR copies of the same fifteen-line endpoint
 * probe - both sides of {@code checkEdges}, plus the far-endpoint load in {@code checkIncomingEdges} and
 * {@code checkOutgoingEdges} - and one of them had drifted: {@code checkEdges}' incoming side flagged the MISSING
 * VERTEX corrupted alongside the edge, where its outgoing twin flagged only the edge. So the same damage reported
 * differently depending on which end of the edge had lost its vertex.
 * <p>
 * The drift also picked the wrong side. A RID that raised {@code RecordNotFoundException} is not corruption, it is
 * absence, and the distinction is expensive rather than cosmetic: every RID in {@code corruptedRecords} puts its
 * BUCKET into {@code affectedBuckets}, and {@code FIX} drops and rebuilds every index on it. Flagging the absent
 * vertex therefore had a dangling-edge sweep after a bulk delete rebuild the indexes of every vertex bucket the
 * deleted vertices lived in - and bought nothing, because the repair loop's delete of a RID that is not there
 * raises {@code RecordNotFoundException} and is ignored. That is the same rule #5680/#5764 settled for a
 * hand-typed RID in the {@code RECORD} scope.
 * <p>
 * These tests pin the resulting contract from both ends: an ABSENT endpoint is reported and never flagged, an
 * UNREADABLE one still is, and the two directions answer identically.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5777DanglingEndpointScopeTest extends TestHelper {
  /** These tests deliberately delete records underneath live edges, so the blanket end-of-test check would fire. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  /**
   * THE ASYMMETRY. An edge whose IN vertex is gone and an edge whose OUT vertex is gone are the same finding, so
   * the pass must flag the same thing for both: the edge, and only the edge.
   * <p>
   * Before the fix the incoming side additionally flagged {@code edge.getIn()}, so this run reported three
   * corrupted records for two dangling edges - and which three depended on which end had been deleted.
   */
  @Test
  void bothDirectionsFlagTheEdgeAndNothingElse() {
    final Graph graph = createGraph();

    // One edge loses its IN endpoint (the company), the other its OUT endpoint (the person).
    rawDelete(graph.company1);
    rawDelete(graph.person2);

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .checkEdges("WorksAt", false, 0, 100, 100);

    assertThat((Collection<RID>) stats.get("corruptedRecords"))
        .as("only the two dangling EDGES are corrupt, neither absent vertex: %s", stats)
        .containsExactlyInAnyOrder(graph.edge1, graph.edge2);
    assertThat((Long) stats.get("totalCorruptedRecords"))
        .as("and the total must not count the phantoms either: %s", stats).isEqualTo(2L);
    // Both sides still count the dangling link, which is the number that DOES describe this damage.
    assertThat((Long) stats.get("invalidLinks")).as("%s", stats).isEqualTo(2L);
  }

  /**
   * The finding is not lost by not being flagged: {@code trackMissingReference} is the channel built for it, and it
   * names both absent endpoints with the count of edges that referenced them.
   */
  @Test
  void theAbsentEndpointsAreStillReportedToTheOperator() {
    final Graph graph = createGraph();

    rawDelete(graph.company1);
    rawDelete(graph.person2);

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .checkEdges("WorksAt", false, 0, 100, 100);

    assertThat((Map<RID, Long>) stats.get("missingReferences"))
        .as("each absent vertex is named once, with its reference count: %s", stats)
        .containsOnly(entryOf(graph.company1), entryOf(graph.person2));

    final Collection<String> warnings = (Collection<String>) stats.get("warnings");
    assertThat(warnings).as("%s", stats).anyMatch(w -> w.equals(
        "edge " + graph.edge1 + " points to the incoming vertex " + graph.company1 + " that is not found (deleted?)"));
    assertThat(warnings).as("%s", stats).anyMatch(w -> w.equals(
        "edge " + graph.edge2 + " points to the outgoing vertex " + graph.person2 + " that is not found (deleted?)"));
  }

  /**
   * THE COST, measured where an operator pays it. {@code rebuiltIndexes} is the set {@code FIX} drops and rebuilds,
   * derived from the buckets of the corrupted records, and a vertex that is not there must not put its bucket in
   * it - a full scan per index on that bucket, bought with a phantom.
   * <p>
   * Run through the full {@code CHECK DATABASE} rather than the graph checker alone, because the derivation from
   * {@code corruptedRecords} to {@code affectedBuckets} to {@code rebuiltIndexes} lives in {@code DatabaseChecker}
   * and is the whole point of the distinction.
   */
  @Test
  void anAbsentEndpointDoesNotDragItsBucketIntoTheIndexRebuild() {
    final Graph graph = createGraph();

    rawDelete(graph.person2);

    // The BUCKET-level names, which is what rebuiltIndexes reports - not the "Person[name]" TypeIndex wrapper the
    // schema is asked for by name. Asserting the wrapper's name would never have matched and would have passed
    // whatever the checker did.
    final Set<String> personIndexes = indexNamesOnBucketOf(graph.person2);
    assertThat(personIndexes).as("precondition: the Person bucket must carry an index there is a cost to rebuild")
        .isNotEmpty();

    final Result row = check("CHECK DATABASE");

    assertThat((Collection<String>) row.getProperty("rebuiltIndexes"))
        .as("no index on the Person bucket may be rebuilt over a Person that is not there: %s", row.toJSON())
        .doesNotContainAnyElementsOf(personIndexes);
    assertThat((Collection<RID>) row.getProperty("corruptedRecords"))
        .as("the dangling edge is the finding, the absent vertex is not: %s", row.toJSON())
        .contains(graph.edge2).doesNotContain(graph.person2);
  }

  /**
   * The other half of the contract: an endpoint whose record IS there and cannot be READ is corruption, and stays
   * flagged - together with the edge. Rebuilding the indexes on ITS bucket is exactly what a corrupt record in that
   * bucket calls for, so the expensive path is still taken where it is earned.
   */
  @Test
  void anUnreadableEndpointIsStillFlaggedCorrupted() {
    final Graph graph = createGraph();

    // Present in its slot, decodable by nobody: the endpoint load fails with something that is NOT
    // RecordNotFoundException, which is the arm that must keep flagging the vertex.
    //
    // TRUNCATION rather than a bogus record-type byte, which does NOT work here: an endpoint is resolved by
    // getOutVertex() as lookupByRID(rid, false), which types the record from its BUCKET, and the deferred
    // asVertex(true) then reads the buffer without ever re-reading the type byte. A record shorter than the fixed
    // vertex prefix is the shape that fails in that deferred read.
    shrinkRecordBuffer(graph.person2);
    // The endpoint load resolves through lookupByRID, which answers from the record cache, and person2 is in it
    // from the fixture. Reopening is what makes the check read the bytes this test just wrote.
    reopenDatabase();

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .checkEdges("WorksAt", false, 0, 100, 100);

    assertThat((Collection<RID>) stats.get("corruptedRecords"))
        .as("a record that is there but unreadable is corrupt, and so is the edge that points at it: %s", stats)
        .contains(graph.edge2, graph.person2);
    assertThat((Collection<String>) stats.get("warnings")).as("%s", stats).anyMatch(w -> w.startsWith(
        "edge " + graph.edge2 + " points to the outgoing vertex " + graph.person2 + " which cannot be loaded (error:"));
  }

  /**
   * The same rule in the OTHER family of caller. {@code checkVertices} walks each vertex's adjacency lists and loads
   * the FAR endpoint of every entry; those two copies flagged the absent far vertex too. The dangling entry is still
   * pruned - that repair is unaffected - but the RID that is not there is no longer called corrupt.
   */
  @Test
  void theAdjacencyWalkAlsoStopsFlaggingAnAbsentFarEndpoint() {
    final Graph graph = createGraph();

    // company1 is edge1's IN endpoint, so walking person1's OUT list loads a vertex that is gone.
    rawDelete(graph.company1);

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .checkVertices("Person", null, false, 0, 100, 100);

    assertThat((Collection<RID>) stats.get("corruptedRecords"))
        .as("the walk flags the edge it cannot follow, not the vertex that is not there: %s", stats)
        .contains(graph.edge1).doesNotContain(graph.company1);
    assertThat((Map<RID, Long>) stats.get("missingReferences"))
        .as("still reported, through the channel meant for it: %s", stats).containsKey(graph.company1);
  }

  /**
   * FIX still repairs what there is to repair, and reports one repair per record it actually removed. The absent
   * vertex contributes nothing to {@code autoFix} - it never did, because the count happens after the delete
   * returns (#6128) and a delete of a RID that is not there raises - which is precisely why flagging it was pure
   * cost.
   */
  @Test
  void fixStillRemovesTheDanglingEdgeAndCountsItOnce() {
    final Graph graph = createGraph();

    rawDelete(graph.person2);

    final Result row = check("CHECK DATABASE TYPE WorksAt FIX");

    assertThat(longProperty(row, "autoFix")).as("one record removed: %s", row.toJSON()).isEqualTo(1L);
    assertThat((Collection<RID>) row.getProperty("deletedRecordsAfterFix"))
        .as("%s", row.toJSON()).containsExactly(graph.edge2);
    database.transaction(() -> assertThat(database.existsRecord(graph.edge2))
        .as("the dangling edge must be gone").isFalse());
  }

  /** The names {@code rebuiltIndexes} would use for the indexes attached to {@code rid}'s bucket. */
  private Set<String> indexNamesOnBucketOf(final RID rid) {
    return Arrays.stream(database.getSchema().getIndexes())
        .filter(index -> index.getAssociatedBucketId() == rid.getBucketId())
        .map(Index::getName).collect(Collectors.toSet());
  }

  private record Graph(RID person1, RID person2, RID company1, RID company2, RID edge1, RID edge2) {
  }

  /**
   * Two disjoint person-to-company edges, so one can lose its IN endpoint and the other its OUT endpoint in the
   * same run without the two findings overlapping. The unique index on {@code Person.name} is what makes
   * {@code rebuiltIndexes} observable.
   */
  private Graph createGraph() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person", 1).createProperty("name", Type.STRING)
          .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
      database.getSchema().createVertexType("Company", 1);
      database.getSchema().createEdgeType("WorksAt", 1);
    });

    final RID[] rids = new RID[6];
    database.transaction(() -> {
      final MutableVertex person1 = database.newVertex("Person").set("name", "p1").save();
      final MutableVertex person2 = database.newVertex("Person").set("name", "p2").save();
      final MutableVertex company1 = database.newVertex("Company").set("name", "c1").save();
      final MutableVertex company2 = database.newVertex("Company").set("name", "c2").save();

      rids[0] = person1.getIdentity();
      rids[1] = person2.getIdentity();
      rids[2] = company1.getIdentity();
      rids[3] = company2.getIdentity();
      rids[4] = person1.newEdge("WorksAt", company1).getIdentity();
      rids[5] = person2.newEdge("WorksAt", company2).getIdentity();
    });

    return new Graph(rids[0], rids[1], rids[2], rids[3], rids[4], rids[5]);
  }

  /**
   * Removes the record through the BUCKET, leaving every reference to it - the adjacency entries and the index
   * entries - behind. That is the shape a check meets after a crash or a raw repair, and the only way to produce a
   * RID that resolves to nothing while an edge still names it.
   */
  private void rawDelete(final RID rid) {
    database.transaction(() -> database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid));
  }

  /**
   * Replaces the record-size varint with a single-byte varint encoding a size far below the fixed vertex prefix:
   * the record still occupies its slot and still resolves, but cannot be DECODED. zigzag(8) == 16.
   */
  private void shrinkRecordBuffer(final RID rid) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = rid.getBucketId();
    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(fileId);
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
    final int maxRecordsInPage = bucket.getMaxRecordsInPage();

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, pageId), pageSize, false);
        final int slotOffset = Binary.SHORT_SERIALIZED_SIZE + (positionInPage * Binary.INT_SERIALIZED_SIZE);
        final int recordOffset = (int) page.readUnsignedInt(slotOffset);
        assertThat(recordOffset).as("the record must still occupy its slot").isGreaterThan(0);
        page.writeByte(recordOffset, (byte) 16);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static Map.Entry<RID, Long> entryOf(final RID rid) {
    return Map.entry(rid, 1L);
  }

  private Result check(final String command) {
    try (final ResultSet rs = database.command("sql", command)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next();
    }
  }

  private static long longProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    assertThat(value).as("check database must report '%s': %s", name, row.toJSON()).isNotNull();
    return ((Number) value).longValue();
  }
}
