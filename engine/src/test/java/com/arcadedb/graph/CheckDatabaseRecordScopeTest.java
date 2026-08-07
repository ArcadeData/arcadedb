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
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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

    // The clause conflict is diagnosed BEFORE the RIDs are resolved, so a scope that also fails to resolve
    // reports the combination - the outer mistake - rather than the resolution failure.
    assertThatThrownBy(() -> database.command("sql", "CHECK DATABASE TYPE Hub RECORD {\"@rid\": null}"))
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

  /**
   * A `RECORD` list that names records but resolves to none must be refused, not quietly widened. An empty scope
   * reads as "no scope" to the checker, which would then run a FULL database check - the one outcome a caller who
   * explicitly named records cannot have wanted, and on a large database enormously more expensive than what was
   * asked for. Not reachable through the literal-RID grammar (a literal always resolves), so this drives the
   * checker directly, which is also the public API surface where it IS reachable.
   */
  @Test
  void anEmptyResolvedRecordScopeIsRefusedRatherThanWidenedToTheWholeDatabase() {
    createSchema();
    createHub();

    // The non-literal RID form resolves through an expression, and an expression can answer null - so unlike the
    // `#n:n` literal this IS reachable from SQL.
    assertThatThrownBy(() -> database.command("sql", "CHECK DATABASE RECORD {\"@rid\": null}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("none of the records given resolves to a RID");

    // And the same guard protects the public API, not just the SQL layer: setRecords is the last place that can
    // still tell "named records, none usable" from "named no records".
    assertThatThrownBy(() -> new DatabaseChecker(database).setRecords(new LinkedHashSet<>(Collections.singletonList(null))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("none of the records given resolves to a RID");

    // A genuinely empty scope still means "no scope", and must NOT be refused.
    assertThatCode(() -> new DatabaseChecker(database).setRecords(Collections.emptySet()))
        .doesNotThrowAnyException();
    assertThatCode(() -> new DatabaseChecker(database).setRecords(null)).doesNotThrowAnyException();
  }

  /**
   * A PARTIAL drop narrows the scope instead of widening it, so it is not refused - but it must not be silent
   * either. A caller who mistyped one of several RIDs would otherwise get a clean report for a check that quietly
   * skipped it.
   */
  @Test
  void checkDatabaseRecordReportsRidsItHadToDrop() {
    createSchema();
    final RID hubRID = createHub();
    createEdges(hubRID, 5);

    final Set<RID> scoped = new LinkedHashSet<>();
    scoped.add(hubRID);
    scoped.add(null);

    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).setRecords(scoped).check();

    assertThat((Collection<String>) result.get("warnings")).as("%s", result)
        .anyMatch(w -> w.contains("one or more of the records given did not resolve"));
    // The valid RID was still checked: narrowing is the point, silence is not.
    assertThat((Long) result.get("totalWarnings")).isEqualTo(1L);
  }

  /**
   * COMPRESS is not limited by the scope and cannot be - it works on buckets, not records. The combination stays
   * legal, because "check this record, then compress the database" is a meaningful thing to ask, but naming a
   * record sets an expectation of a bounded run and this is the one clause that breaks it. So it warns.
   */
  @Test
  void checkDatabaseRecordWarnsThatCompressIsNotScoped() {
    createSchema();
    final RID hubRID = createHub();
    createEdges(hubRID, 5);

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE RECORD " + hubRID + " COMPRESS")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Collection<String>) row.getProperty("warnings")).as("%s", row.toJSON())
          .anyMatch(w -> w.contains("COMPRESS is not limited by the RECORD scope"));
    }

    // Without COMPRESS the same scope stays silent, so the warning tracks the clause and not the scope.
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE RECORD " + hubRID)) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Collection<String>) row.getProperty("warnings")).as("%s", row.toJSON()).isEmpty();
    }
  }

  /**
   * The retained warnings are capped, while the totals keep counting - so a scope naming a flood of bogus RIDs
   * reports how many problems there were without holding a message for each.
   */
  @Test
  void checkDatabaseRecordCapsTheWarningsItRetains() {
    createSchema();
    createHub();

    final DatabaseChecker checker = new DatabaseChecker(database).setVerboseLevel(0).setMaxWarnings(10);
    final Set<RID> scoped = new LinkedHashSet<>();
    for (int i = 0; i < 40; i++)
      scoped.add(database.newRID(9999, i));

    final Map<String, Object> result = checker.setRecords(scoped).check();

    assertThat((Collection<String>) result.get("warnings")).as("retained warnings are capped").hasSize(10);
    assertThat((Long) result.get("totalWarnings")).as("but every occurrence is counted").isEqualTo(40L);
  }

  /**
   * The other half of the cost story, made executable: a listed record that is GENUINELY corrupted (not merely
   * missing, and not merely chain-broken) is flagged, and under {@code FIX} every index on its bucket is dropped
   * and rebuilt. That is the documented limit of the scope - `RECORD` bounds the CHECK, not necessarily the FIX -
   * and pinning it means a future change cannot quietly make a scoped repair either cheaper or more destructive
   * than stated.
   */
  @Test
  void checkDatabaseRecordFixRebuildsTheBucketIndexesOfAGenuinelyCorruptedRecord() {
    createSchema();
    database.transaction(() -> {
      database.getSchema().getType("Src").createProperty("name", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Src", "name");
    });

    final RID[] victim = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("Src").set("name", "corrupt-me");
      v.save();
      victim[0] = v.getIdentity();
    });

    shrinkRecordBuffer(victim[0]);

    // Precondition: the record still occupies its slot (so this is NOT the missing-RID case) but no longer reads
    // back as a vertex.
    database.transaction(() -> {
      assertThat(database.existsRecord(victim[0])).as("the record must still be there").isTrue();
      assertThatThrownBy(() -> database.lookupByRID(victim[0], true).asVertex(true))
          .isNotInstanceOf(RecordNotFoundException.class);
    });

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE RECORD " + victim[0] + " FIX")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(longProperty(row, "totalCorruptedRecords")).as("genuine corruption IS flagged: %s", row.toJSON())
          .isGreaterThan(0L);
      // The documented cost: the record's bucket had its indexes dropped and rebuilt.
      assertThat((Collection<String>) row.getProperty("rebuiltIndexes")).as("%s", row.toJSON()).isNotEmpty();
    }
  }

  /**
   * #5764, item 4: the scoped arm materialises the record through {@code lookupByRID} and the type-wide arm through
   * a raw bucket scan, so a corruption shape that surfaced only through the raw view would be caught by one and
   * missed by the other. It cannot: {@code lookupByRID} IS that materialisation
   * ({@code newImmutableRecord(type, rid, bucket.getRecord(rid).copyOfContent())}), and both then decode through the
   * same {@code asVertex(true)}. Pinned rather than argued, so a future change to either enumeration that breaks the
   * equivalence fails here.
   */
  @Test
  void theScopedAndTypeWideRunsAgreeOnAGenuinelyCorruptedRecord() {
    createSchema();

    final RID[] victim = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("Src").set("name", "corrupt-me");
      v.save();
      victim[0] = v.getIdentity();
    });

    shrinkRecordBuffer(victim[0]);

    // Neither run is allowed to FIX: the point is that the two CHECKS see the same thing, and a fix would delete
    // the record out from under the second one.
    final long scopedCorrupted = corruptedReportedBy("CHECK DATABASE RECORD " + victim[0], victim[0]);
    final long typeWideCorrupted = corruptedReportedBy("CHECK DATABASE TYPE Src", victim[0]);

    assertThat(scopedCorrupted).as("the scoped run must flag the corrupted record").isEqualTo(1L);
    assertThat(typeWideCorrupted).as("and the type-wide run must agree, not merely also complain")
        .isEqualTo(scopedCorrupted);
  }

  /**
   * #5764, item 3: the same corrupt DOCUMENT must read the same whichever path found it. The type-wide arm used to
   * add to the warning and corrupted-record sets without touching {@code totalWarnings}/{@code totalCorruptedRecords}
   * - so a run reported zero of both while listing the finding - and called the document a "vertex" it was
   * "removing", when nothing on that path removes anything.
   */
  @Test
  void aCorruptDocumentIsReportedIdenticallyByTheScopedAndTypeWideRuns() {
    createSchema();
    database.transaction(() -> database.getSchema().createDocumentType("Doc"));

    final RID[] victim = new RID[1];
    database.transaction(() -> victim[0] = database.newDocument("Doc").set("k", 1).save().getIdentity());

    corruptRecordTypeByte(victim[0]);

    final String expected = "document " + victim[0] + " cannot be loaded";

    for (final String command : List.of("CHECK DATABASE RECORD " + victim[0], "CHECK DATABASE TYPE Doc")) {
      try (final ResultSet rs = database.command("sql", command)) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat((Collection<String>) row.getProperty("warnings")).as("%s: %s", command, row.toJSON())
            .contains(expected);
        assertThat(longProperty(row, "totalWarnings")).as("%s must COUNT what it reports: %s", command, row.toJSON())
            .isGreaterThan(0L);
        assertThat(longProperty(row, "totalCorruptedRecords")).as("%s: %s", command, row.toJSON()).isEqualTo(1L);
      }
    }
  }

  /** Runs a non-fixing check and returns its corrupted-record total, asserting the record was named in a warning. */
  private long corruptedReportedBy(final String command, final RID rid) {
    try (final ResultSet rs = database.command("sql", command)) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Collection<String>) row.getProperty("warnings")).as("%s: %s", command, row.toJSON())
          .anyMatch(w -> w.contains(rid.toString()));
      assertThat((Collection<?>) row.getProperty("rebuiltIndexes")).as("no FIX, so nothing may be rebuilt: %s",
          row.toJSON()).isEmpty();
      return longProperty(row, "totalCorruptedRecords");
    }
  }

  /**
   * Overwrites the record-type byte of {@code rid} with a value no {@code RecordFactory} branch knows, so the record
   * still occupies its slot and still has a valid size, but cannot be materialised at all - the one corruption shape
   * that reaches BOTH the raw-view construction of the type-wide scan and the {@code lookupByRID} of the scoped arm
   * at the same point, which is what makes the two paths comparable.
   */
  private void corruptRecordTypeByte(final RID rid) {
    onRecordPage(rid, (page, recordOffset) -> {
      final long[] recordSize = page.readNumberAndSize(recordOffset);
      page.writeByte((int) (recordOffset + recordSize[1]), (byte) 99);
    });
  }

  /**
   * Replaces the record-size varint of {@code rid} with a single-byte varint encoding a size far below the fixed
   * 25-byte vertex prefix, so the record still occupies its slot but can no longer be read back as a vertex - the
   * on-disk shape of a corrupted (as opposed to a missing) record. zigzag(8) == 16.
   */
  private void shrinkRecordBuffer(final RID rid) {
    onRecordPage(rid, (page, recordOffset) -> page.writeByte(recordOffset, (byte) 16));
  }

  /**
   * Runs {@code mutation} against the page holding {@code rid}, handing it the content-relative offset where the
   * record's size varint starts. Shared by the corruption helpers so they agree on the page layout.
   */
  private void onRecordPage(final RID rid, final BiConsumer<MutablePage, Integer> mutation) {
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
        // PAGE_RECORD_TABLE_OFFSET == PAGE_RECORD_COUNT_IN_PAGE_OFFSET(0) + SHORT_SERIALIZED_SIZE; each slot holds
        // the content-relative offset of its record, whose first byte starts the record-size varint.
        final int slotOffset = Binary.SHORT_SERIALIZED_SIZE + (positionInPage * Binary.INT_SERIALIZED_SIZE);
        final int recordOffset = (int) page.readUnsignedInt(slotOffset);
        assertThat(recordOffset).as("the record must still occupy its slot").isGreaterThan(0);
        mutation.accept(page, recordOffset);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
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
