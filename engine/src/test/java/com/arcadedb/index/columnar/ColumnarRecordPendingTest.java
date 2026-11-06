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
package com.arcadedb.index.columnar;

import com.arcadedb.database.Binary;
import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Task 15 of plan 7 (#10662): a local commit marks the records it wrote instead of their whole pages, and the apply pass and the
 * pending positions of a query follow those marks. Every write a transaction makes to a record it did not write (a neighbour moved off
 * the page, a chunk tail moved, a placeholder's content chunked) must still reach the facets: FACETS equal GROUP BY with the apply
 * held and again once it caught up, under every mutation kind.
 */
class ColumnarRecordPendingTest extends BucketPageLayoutTestSupport {
  private static final String BIG               = "b".repeat(20 * 1024);
  private static final String HUGE              = "h".repeat(100 * 1024);
  /** A placeholder pointer: the 1-byte RECORD_PLACEHOLDER_POINTER marker plus the 8-byte position. */
  private static final int    POINTER_FOOTPRINT = 1 + Binary.LONG_SERIALIZED_SIZE;
  private static final int    SLOTS_IN_PAGE     = 2048;
  /**
   * A page of 4 KB of content, the smallest a bucket accepts since #11286 (the #10859 geometry, which used 1 KB): records no bigger
   * than a pointer fill it before its slot table does.
   */
  private static final int    SMALL_PAGE_SIZE   =
      BasePage.PAGE_HEADER_SIZE + Binary.SHORT_SERIALIZED_SIZE + SLOTS_IN_PAGE * Binary.INT_SERIALIZED_SIZE + 4096;

  @Test
  void pendingPositionsAreTheWrittenRecordsOnly() throws Exception {
    final List<RID> rids = createWithRecords("Written", 40);
    FacetOracle.awaitCaughtUp(database);

    final RID[] inserted = new RID[1];
    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> {
        rids.get(3).asDocument(true).modify().set("brand", "x3").save();
        rids.get(17).asDocument(true).modify().set("brand", "a much longer brand that grows the record in its page").save();
        rids.get(20).asDocument(true).delete();
        inserted[0] = database.newDocument("Written").set("brand", "new").save().getIdentity();
      });
      assertThat(page(inserted[0])).as("the premise: the insert reused page 0").isZero();

      assertThat(pendingOf("Written")).containsExactlyInAnyOrder(rids.get(3).getPosition(), rids.get(17).getPosition(),
          rids.get(20).getPosition(), inserted[0].getPosition());
      final FacetResults results = assertFacetsExact("Written", "while the apply is held");
      // the two updated records and the new one are read from their records, not the 37 others of the page
      assertThat(results.patched()).isEqualTo(3);
    }
    FacetOracle.awaitCaughtUp(database);
    assertFacetsExact("Written", "caught up");
  }

  @Test
  void slotMarkedPageReReadsOnlyMarkedSlots() throws Exception {
    final List<RID> rids = createWithRecords("Reread", 200);
    final int maxRecordsInPage = bucket("Reread").getMaxRecordsInPage();
    // the premise: the 200 records share one page, so a page-granular mark would re-read all 200
    for (final RID rid : rids)
      assertThat(rid.getPosition() / maxRecordsInPage).as(rid.toString()).isZero();
    FacetOracle.awaitCaughtUp(database);
    final ColumnarStats stats = manager().getStats();

    final long before = stats.recordsRereadCount("Reread");
    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> {
        for (final int i : new int[] { 5, 77, 150 })
          rids.get(i).asDocument(true).modify().set("brand", "changed" + i).save();
      });
    }
    FacetOracle.awaitCaughtUp(database);
    assertThat(stats.recordsRereadCount("Reread") - before).as("the written records only").isEqualTo(3);
    assertFacetsExact("Reread", "caught up");
  }

  @Test
  void overflowPastSixteenSlotsMarksTheWholePage() throws Exception {
    final int limit = ColumnarIndexManager.MAX_SLOT_MARKS_PER_PAGE;
    final List<RID> rids = createWithRecords("Overflow", 100);
    final int maxRecordsInPage = bucket("Overflow").getMaxRecordsInPage();
    FacetOracle.awaitCaughtUp(database);

    try (final AutoCloseable hold = manager().suspendApply()) {
      updateBrands(rids, 0, limit, "at-limit");
      assertThat(pendingOf("Overflow")).as("sixteen records stay records").hasSize(limit);
      assertFacetsExact("Overflow", "sixteen records");
    }
    FacetOracle.awaitCaughtUp(database);

    try (final AutoCloseable hold = manager().suspendApply()) {
      updateBrands(rids, 0, limit + 1, "past-limit");
      assertThat(pendingOf("Overflow")).as("seventeen records of one commit: the whole page").hasSize(maxRecordsInPage);
      assertFacetsExact("Overflow", "seventeen records");
    }
    FacetOracle.awaitCaughtUp(database);
    assertFacetsExact("Overflow", "caught up after seventeen");

    try (final AutoCloseable hold = manager().suspendApply()) {
      // two commits of ten records each in the same epoch: the page escalates on the second
      updateBrands(rids, 20, 10, "first");
      assertThat(pendingOf("Overflow")).hasSize(10);
      updateBrands(rids, 40, 10, "second");
      assertThat(pendingOf("Overflow")).as("twenty records of two commits: the whole page").hasSize(maxRecordsInPage);
      assertFacetsExact("Overflow", "two commits");
    }
    FacetOracle.awaitCaughtUp(database);
    assertFacetsExact("Overflow", "caught up after two commits");
  }

  /**
   * Task 15b: a page read by record is stamped with the version read, so a flushed page is not taken for changed after a crash; but
   * not when a mark of the page landed after the drain, nor while an apply has written pages it has not marked yet: the write the read
   * saw may be one no drained mark names, and the block then keeps its older version.
   */
  @Test
  void aPageReadByRecordIsStampedWithTheVersionReadUnlessAMarkIsStillToCome() throws Exception {
    final List<RID> rids = createWithRecords("Stamped", 20);
    final int bucketId = bucketId("Stamped");
    final ColumnarProjection projection = manager().projection(bucketId);
    FacetOracle.awaitCaughtUp(database);
    final int page = page(rids.getFirst());

    try (final AutoCloseable hold = manager().suspendApply()) {
      // control: no mark to come, the block stands for the version read
      updateBrands(rids, 1, 1, "first");
      ColumnarApplier.runPass(manager(), projection);
      final long stamped = projection.overlay().page(page).pageVersion();
      assertThat(stamped).isEqualTo(committedVersion(bucketId, page));

      // a mark of the page lands between the drain and the stamp (another commit's): the block keeps its older version
      updateBrands(rids, 2, 1, "second");
      ColumnarTestHooks.killPoint = point -> {
        if (ColumnarApplier.AFTER_PAGE_READ.equals(point))
          manager().markRecords(bucketId, new long[] { rids.get(3).getPosition() }, 1);
      };
      try {
        ColumnarApplier.runPass(manager(), projection);
      } finally {
        ColumnarTestHooks.killPoint = null;
      }
      assertThat(committedVersion(bucketId, page)).isGreaterThan(stamped);
      assertThat(projection.overlay().page(page).pageVersion()).as("re-marked after the drain").isEqualTo(stamped);

      // an apply that writes before it marks (a Raft apply, a forwarded write) is between the two: the same
      updateBrands(rids, 4, 1, "third");
      manager().unmarkedApplyStarted();
      try {
        ColumnarApplier.runPass(manager(), projection);
      } finally {
        manager().unmarkedApplyEnded();
      }
      assertThat(projection.overlay().page(page).pageVersion()).as("an apply not marked yet").isEqualTo(stamped);

      // nothing to come any more: the next read by record stamps the version it read
      updateBrands(rids, 5, 1, "fourth");
      ColumnarApplier.runPass(manager(), projection);
      assertThat(projection.overlay().page(page).pageVersion()).isEqualTo(committedVersion(bucketId, page));
      assertFacetsExact("Stamped", "stamped, apply held");
    }
    FacetOracle.awaitCaughtUp(database);
    assertFacetsExact("Stamped", "stamped, caught up");
  }

  /**
   * Task 15b: the owners of a content page re-read alone on a page with no overlay block (it was flushed) follow the same rule as a page
   * read by record: the version read, unless a mark of their page is still to come, then 0.
   */
  @Test
  void ownersReadOnACleanPageFollowTheSameStampRule() throws Exception {
    final RID[] rids = createSmallSlotNextToANeighbour("Owners");
    database.transaction(() -> rids[0].asDocument(true).modify().set("brand", "grown").set("v", BIG).save());
    assertThat((Long) bucketStats("Owners").get("totalPlaceholderRecords")).as("the premise: the neighbour is behind a pointer")
        .isEqualTo(2L);
    final int bucketId = bucketId("Owners");
    final ColumnarProjection projection = manager().projection(bucketId);
    final int ownerPage = page(rids[1]);

    // control: an update of the neighbour writes only its content page; its owner is re-read on a page with no block
    FacetOracle.awaitCaughtUp(database);
    manager().flushNow();
    try (final AutoCloseable hold = manager().suspendApply()) {
      assertThat(projection.overlay().page(ownerPage)).as("the premise: no block on the owner's page").isNull();
      database.transaction(() -> rids[1].asDocument(true).modify().set("brand", "n1").save());
      ColumnarApplier.runPass(manager(), projection);
      assertThat(projection.overlay().page(ownerPage).pageVersion()).isEqualTo(committedVersion(bucketId, ownerPage));
    }

    // a mark of the owner's page lands after the drain: 0, so an unclean open or a rebuild re-applies the page
    FacetOracle.awaitCaughtUp(database);
    manager().flushNow();
    try (final AutoCloseable hold = manager().suspendApply()) {
      assertThat(projection.overlay().page(ownerPage)).isNull();
      database.transaction(() -> rids[1].asDocument(true).modify().set("brand", "n2").save());
      ColumnarTestHooks.killPoint = point -> {
        if (ColumnarApplier.AFTER_PAGE_READ.equals(point))
          manager().markPages(bucketId, new int[] { ownerPage }, 1);
      };
      try {
        ColumnarApplier.runPass(manager(), projection);
      } finally {
        ColumnarTestHooks.killPoint = null;
      }
      assertThat(projection.overlay().page(ownerPage).pageVersion()).as("a mark of the owner's page to come").isZero();
      assertFacetsExact("Owners", "owners, apply held");
    }
    FacetOracle.awaitCaughtUp(database);
    assertFacetsExact("Owners", "owners, caught up");
  }

  @Test
  void aStatementTakenBackMarksItsPageWhole() throws Exception {
    createType("Savepoint");
    database.command("sql", "CREATE PROPERTY Savepoint.k INTEGER");
    database.command("sql", "CREATE INDEX ON Savepoint (k) UNIQUE");
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 30; i++)
        rids.add(database.newDocument("Savepoint").set("brand", "b" + (i % 5)).set("k", i).save().getIdentity());
    });
    final int maxRecordsInPage = bucket("Savepoint").getMaxRecordsInPage();
    FacetOracle.awaitCaughtUp(database);

    try (final AutoCloseable hold = manager().suspendApply()) {
      database.begin();
      // a tracked write of the transaction on page 0, the shape a record mark is made of...
      rids.get(3).asDocument(true).modify().set("brand", "kept").save();
      database.newDocument("Savepoint").set("brand", "kept").set("k", 500).save();
      // ...then a statement that inserts on the same page and fails on its last row (the key the transaction wrote above): it is taken
      // back, its page restored byte by byte
      assertThatThrownBy(() -> database.command("sql",
          "INSERT INTO Savepoint (brand, k) VALUES ('taken back', 100), ('taken back', 101), ('taken back', 500)").close())
          .isInstanceOf(DuplicatedKeyException.class);
      database.newDocument("Savepoint").set("brand", "after").set("k", 200).save();
      database.commit();

      // the restored page is poisoned for the slot merge: no record list describes it any more, so it is marked whole
      assertThat(pendingOf("Savepoint")).as("the page of the statement taken back").hasSize(maxRecordsInPage);
      assertFacetsExact("Savepoint", "a statement taken back, apply held");
    }
    FacetOracle.awaitCaughtUp(database);
    assertFacetsExact("Savepoint", "a statement taken back, caught up");
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Savepoint WHERE brand = 'taken back'")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  @Test
  void recordPendingOracleUnderEveryMutationKind() throws Exception {
    // plain records on a page shared with unchanged neighbours
    final List<RID> plain = createWithRecords("Plain", 30);
    exactWhileHeldAndCaughtUp("Plain", "insert", () -> database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("Plain").set("brand", "inserted").save();
    }));
    exactWhileHeldAndCaughtUp("Plain", "update", () -> updateBrands(plain, 1, 3, "updated"));
    exactWhileHeldAndCaughtUp("Plain", "slot reuse in one pass", () -> {
      database.transaction(() -> plain.get(12).asDocument(true).delete());
      database.transaction(() -> assertThat(database.newDocument("Plain").set("brand", "reused").save().getIdentity())
          .as("the premise: the freed slot is taken again").isEqualTo(plain.get(12)));
    });
    exactWhileHeldAndCaughtUp("Plain", "delete", () -> database.transaction(() -> {
      plain.get(10).asDocument(true).delete();
      plain.get(11).asDocument(true).delete();
    }));
    exactWhileHeldAndCaughtUp("Plain", "delete and reinsert in one transaction", () -> database.transaction(() -> {
      plain.get(13).asDocument(true).delete();
      database.newDocument("Plain").set("brand", "reinserted").save();
    }));
    exactWhileHeldAndCaughtUp("Plain", "rollback", () -> {
      database.begin();
      plain.get(14).asDocument(true).modify().set("brand", "rolled back").save();
      database.newDocument("Plain").set("brand", "rolled back").save();
      database.rollback();
    });

    // a record that grows into chunks, then an update of the grown record
    createWithRecords("Chunked", 10);
    final RID chunked = firstOf("Chunked");
    exactWhileHeldAndCaughtUp("Chunked", "grow into chunks", () -> database.transaction(
        () -> chunked.asDocument(true).modify().set("brand", "chunked").set("v", HUGE).save()));
    assertThat((Long) bucketStats("Chunked").get("totalMultiPageRecords")).as("the premise: a chunk chain").isEqualTo(1L);
    exactWhileHeldAndCaughtUp("Chunked", "update of a grown record", () -> database.transaction(
        () -> chunked.asDocument(true).modify().set("brand", "chunked again").save()));

    // a record that grows into a placeholder on a sealed page (#6149), then an update of it that writes only its content page
    final RID tiny = createSealedPageWithTinyRecord("Placeholder");
    exactWhileHeldAndCaughtUp("Placeholder", "grow into a placeholder", () -> database.transaction(
        () -> tiny.asDocument(true).modify().set("v", BIG).set("brand", "grown").save()));
    assertThat((Long) bucketStats("Placeholder").get("totalPlaceholderRecords")).as("the premise: a placeholder").isEqualTo(1L);
    exactWhileHeldAndCaughtUp("Placeholder", "update of a placeholder", () -> database.transaction(
        () -> tiny.asDocument(true).modify().set("brand", "regrown").save()));

    // a growth that relocates a neighbour off the page (#10637), then an update of that neighbour that writes only its content page
    final RID[] relocation = createSmallSlotNextToANeighbour("Relocated");
    exactWhileHeldAndCaughtUp("Relocated", "a neighbour relocated off the page", () -> database.transaction(
        () -> relocation[0].asDocument(true).modify().set("brand", "grown").set("v", BIG).save()));
    assertThat((Long) bucketStats("Relocated").get("totalPlaceholderRecords")).as("the premise: the neighbour moved behind a pointer")
        .isEqualTo(2L);
    exactWhileHeldAndCaughtUp("Relocated", "update of the relocated neighbour", () -> database.transaction(
        () -> relocation[1].asDocument(true).modify().set("brand", "n2").save()));

    // a growth that moves a chunk tail off the page (#10669), then an update of the chunked neighbour
    final RID[] tail = createSmallSlotNextToAChunkHead("Tail");
    exactWhileHeldAndCaughtUp("Tail", "a chunk tail moved off the page", () -> database.transaction(
        () -> tail[0].asDocument(true).modify().set("brand", "grown").set("v", BIG).save()));
    exactWhileHeldAndCaughtUp("Tail", "update of the chunked neighbour", () -> database.transaction(
        () -> tail[1].asDocument(true).modify().set("brand", "sealer2").save()));

    // a growth that chunks a placeholder's content (#10859), then an update of that placeholder
    final RID[] content = createSmallSlotNextToAPlaceholderContent("Content");
    exactWhileHeldAndCaughtUp("Content", "a placeholder's content chunked", () -> database.transaction(
        () -> content[0].asDocument(true).modify().set("brand", "grown").set("v", BIG).save()));
    assertThat(markerOf(content[2])).as("the premise: the content record is now the head of a chunk chain").isEqualTo(-4L);
    exactWhileHeldAndCaughtUp("Content", "update of the placeholder whose content was chunked", () -> database.transaction(
        () -> content[1].asDocument(true).modify().set("brand", "p2").save()));

    // an external-property value rewritten in place: only the external page changes
    final DocumentType external = database.getSchema().createDocumentType("External", 1);
    external.createProperty("brand", Type.STRING).setExternal(true);
    database.command("sql", "CREATE INDEX ON External (brand) COLUMNAR");
    final List<RID> externals = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        externals.add(database.newDocument("External").set("brand", "e" + (i % 4)).save().getIdentity());
    });
    assertThat(((LocalDocumentType) external).getExternalBucketIdFor(bucketId("External"))).isNotNegative();
    exactWhileHeldAndCaughtUp("External", "external-property update", () -> updateBrands(externals, 2, 3, "e9"));

    // an edge added to a vertex rewrites the vertex record's head pointer; another vertex of the page changes its brand
    database.command("sql", "CREATE VERTEX TYPE Node BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Node.brand STRING");
    database.command("sql", "CREATE EDGE TYPE Link BUCKETS 1");
    database.command("sql", "CREATE INDEX ON Node (brand) COLUMNAR");
    final List<RID> nodes = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        nodes.add(database.newVertex("Node").set("brand", "n" + (i % 3)).save().getIdentity());
    });
    exactWhileHeldAndCaughtUp("Node", "edge added to a vertex", () -> database.transaction(() -> {
      final MutableVertex from = nodes.get(1).asVertex(true).modify();
      from.newEdge("Link", nodes.get(2).asVertex(true));
      nodes.get(3).asVertex(true).modify().set("brand", "linked").save();
    }));
  }

  // ------------------------------------------------------------------------------------------------------------------ fixtures

  /** Runs the mutation with the apply held, checks FACETS against GROUP BY, then again once the apply caught up. */
  private void exactWhileHeldAndCaughtUp(final String type, final String what, final ThrowingRunnable mutation) throws Exception {
    FacetOracle.awaitCaughtUp(database);
    try (final AutoCloseable hold = manager().suspendApply()) {
      mutation.run();
      assertFacetsExact(type, what + ", apply held");
    }
    FacetOracle.awaitCaughtUp(database);
    assertFacetsExact(type, what + ", caught up");
  }

  /** The facet is answered from the projection (never a scan that would hide a wrong projection), exact, and equal to GROUP BY. */
  private FacetResults assertFacetsExact(final String type, final String what) {
    final FacetResults results;
    try (final ResultSet rs = database.query("sql", "SELECT FROM `" + type + "` FACETS f: brand TERMS TOP 1000")) {
      results = rs.getFacets();
    }
    assertThat(results).as(what).isNotNull();
    final FacetResult result = results.get("f");
    assertThat(result.source()).as(what + ": " + result.reason()).isEqualTo("index");
    assertThat(results.exact()).as(what).isTrue();
    try {
      FacetOracle.assertFacetsEqualGroupBy(database, type, "brand", null);
    } catch (final AssertionError e) {
      throw new AssertionError(type + ", " + what + ": " + e.getMessage(), e);
    }
    return results;
  }

  /** A single-bucket type with a COLUMNAR brand and a string v, holding {@code count} records of five brands on its first page. */
  private List<RID> createWithRecords(final String type, final int count) {
    createType(type);
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        rids.add(database.newDocument(type).set("brand", "b" + (i % 5)).save().getIdentity());
    });
    return rids;
  }

  private void createType(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE `" + type + "` BUCKETS 1");
    database.command("sql", "CREATE PROPERTY `" + type + "`.brand STRING");
    database.command("sql", "CREATE PROPERTY `" + type + "`.v STRING");
    database.command("sql", "CREATE INDEX ON `" + type + "` (brand) COLUMNAR");
  }

  /** #6149: a 9-byte record on a page with no free tail, among records with a brand; returns the small record. */
  private RID createSealedPageWithTinyRecord(final String type) {
    createWithRecords(type, 5);
    final RID[] tiny = new RID[1];
    database.transaction(() -> tiny[0] = database.newDocument(type).set("v", "p").save().getIdentity());
    sealFirstPage(type);
    return tiny[0];
  }

  /** #10637: an unpadded empty record, a short neighbour with a brand, then the page sealed; returns both. */
  private RID[] createSmallSlotNextToANeighbour(final String type) {
    createType(type);
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newDocument(type).save().getIdentity();
      rids[1] = database.newDocument(type).set("brand", "nb").set("v", "a short neighbour").save().getIdentity();
    });
    shrinkToUnpaddedSize(rids[0]);
    sealFirstPage(type);
    return rids;
  }

  /** #10669: an unpadded empty record next to a record that spilled into chunks taking the page's free tail; returns both. */
  private RID[] createSmallSlotNextToAChunkHead(final String type) {
    createType(type);
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newDocument(type).save().getIdentity();
      rids[1] = database.newDocument(type).set("v", "s").save().getIdentity();
    });
    shrinkToUnpaddedSize(rids[0]);
    database.transaction(() -> rids[1].asDocument(true).modify().set("brand", "sealer").set("v", "s".repeat(70 * 1024)).save());
    final Map<String, Object> layout = bucketStats(type);
    assertThat((Long) layout.get("totalMultiPageRecords")).as("the premise: the sealing record spilled " + layout).isEqualTo(1L);
    assertThat((Long) layout.get("totalPlaceholderRecords")).as("the premise: no pointer yet " + layout).isZero();
    return rids;
  }

  /**
   * #10859: page 0 of a type with about 1 KB of content holds an unpadded empty record, a placeholder whose content record follows on
   * the same page, and fillers no bigger than a pointer up to the end of the content area, so the only record with bytes to spare is
   * the placeholder's content. Returns the small record, the placeholder and its content position.
   */
  private RID[] createSmallSlotNextToAPlaceholderContent(final String type) {
    database.getSchema().buildDocumentType().withName(type).withTotalBuckets(1).withPageSize(SMALL_PAGE_SIZE).create();
    database.command("sql", "CREATE PROPERTY `" + type + "`.brand STRING");
    database.command("sql", "CREATE PROPERTY `" + type + "`.v STRING");
    database.command("sql", "CREATE INDEX ON `" + type + "` (brand) COLUMNAR");
    final RID[] rids = new RID[3];
    final RID[] template = new RID[1];
    database.transaction(() -> {
      rids[0] = database.newDocument(type).save().getIdentity();
      template[0] = database.newDocument(type).save().getIdentity();
      // empty, so its slot is exactly a pointer's footprint once it is one
      rids[1] = database.newDocument(type).save().getIdentity();
    });
    assertThat(bucket(type).getMaxRecordsInPage()).isEqualTo(SLOTS_IN_PAGE);
    shrinkToUnpaddedSize(rids[0]);

    final DatabaseInternal db = (DatabaseInternal) database;
    final int emptyContent = db.getSerializer().serialize(db, database.newDocument(type)).size();
    final byte[] quoted = db.getSerializer().serialize(db, database.newDocument(type).set("brand", "ph").set("v", "q".repeat(40)))
        .toByteArray();
    database.transaction(() -> onSlot(template[0], page -> {
      final int templateOffset = recordOffsetOf(page, template[0]);
      final long[] templateSize = page.readNumberAndSize(templateOffset);
      final byte[] templateContent = new byte[(int) templateSize[0]];
      page.readByteArray(templateOffset + (int) templateSize[1], templateContent, 0, templateContent.length);

      int slots = page.readShort(0);
      int end = contentEnd(page);

      final int offset = recordOffsetOf(page, rids[1]);
      assertThat(page.readNumberAndSize(offset)).as("the premise: a slot a pointer fills exactly").containsExactly(POINTER_FOOTPRINT - 1,
          1L);
      rids[2] = new RID(template[0].getBucketId(), slots);
      end = appendSlot(page, slots++, end, -quoted.length, quoted, quoted.length, quoted.length);
      assertThat(page.writeNumber(offset, -1L)).isEqualTo(1);
      page.writeLong(offset + 1, rids[2].getPosition());

      // empty documents of an unpadded record's footprint up to a pointer's, never more, ending exactly at the end of the content area
      final int maxShave = POINTER_FOOTPRINT - 1 - emptyContent;
      final int room = page.getMaxContentSize() - end;
      final int records = (room + POINTER_FOOTPRINT - 1) / POINTER_FOOTPRINT;
      int excess = records * POINTER_FOOTPRINT - room;
      for (int i = 0; i < records; i++) {
        final int shave = Math.min(maxShave, excess);
        excess -= shave;
        end = appendSlot(page, slots++, end, POINTER_FOOTPRINT - 1 - shave, templateContent, templateContent.length, emptyContent);
      }
      assertThat(excess).isZero();
      assertThat(end).as("the premise: the page has no free tail").isEqualTo(page.getMaxContentSize());

      page.writeShort(0, (short) slots);
      ((DatabaseInternal) database).getTransaction().updateBucketRecordDelta(template[0].getBucketId(), records);
      return 0L;
    }));
    return rids;
  }

  /** Writes a record of {@code size} bytes (negated for a placeholder's content) at {@code end} and points the slot at it. */
  private static int appendSlot(final MutablePage page, final int slot, final int end, final int size, final byte[] body,
      final int bodyLength, final int serializedLength) {
    final int length = Math.abs(size);
    assertThat(length).isGreaterThanOrEqualTo(serializedLength);
    page.writeUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE, end);
    final int marker = page.writeNumber(end, size);
    final byte[] content = new byte[length];
    System.arraycopy(body, 0, content, 0, Math.min(length, bodyLength));
    page.writeByteArray(end + marker, content, 0, length);
    return end + marker + length;
  }

  /** Where the content of the page ends: past the furthest record, whatever its shape. */
  private static int contentEnd(final MutablePage page) {
    final int slots = page.readShort(0);
    int end = 0;
    for (int slot = 0; slot < slots; slot++) {
      final int offset = (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE);
      if (offset == 0)
        continue;
      final long[] size = page.readNumberAndSize(offset);
      final int footprint;
      if (size[0] > 0)
        footprint = (int) (size[0] + size[1]);
      else if (size[0] == -1L)
        footprint = (int) size[1] + Binary.LONG_SERIALIZED_SIZE;
      else if (size[0] == -2L || size[0] == -3L || size[0] == -4L)
        footprint = (int) size[1] + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE + page.readInt((int) (offset + size[1]));
      else
        footprint = (int) (-size[0] + size[1]);
      end = Math.max(end, offset + footprint);
    }
    return end;
  }

  private long markerOf(final RID rid) {
    final long[] marker = new long[1];
    database.transaction(() -> onSlot(rid, page -> marker[0] = page.readNumberAndSize(recordOffsetOf(page, rid))[0]));
    return marker[0];
  }

  private void updateBrands(final List<RID> rids, final int from, final int count, final String brand) {
    database.transaction(() -> {
      for (int i = from; i < from + count; i++) {
        final MutableDocument document = rids.get(i).asDocument(true).modify();
        document.set("brand", brand).save();
      }
    });
  }

  /** The pending positions of the type's projection, as a query acquiring it now sees them. */
  private List<Long> pendingOf(final String type) {
    final List<Long> positions = new ArrayList<>();
    try (final ColumnarSnapshot snapshot = manager().projection(bucketId(type)).acquire()) {
      snapshot.pendingPositions().forEach(positions::add);
    }
    return positions;
  }

  /** The version of the committed page, as an apply pass reads it (no slot visited). */
  private long committedVersion(final int bucketId, final int page) {
    final LocalBucket bucket = bucket(bucketId);
    return bucket.visitCommittedSlots(page, new long[bucket.getMaxRecordsInPage() / 64], (position, kind, content, offPages, count) -> {
    });
  }

  private RID firstOf(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM `" + type + "` LIMIT 1")) {
      return rs.next().<RID>getProperty("rid");
    }
  }

  private int page(final RID rid) {
    return (int) (rid.getPosition() / bucket(rid.getBucketId()).getMaxRecordsInPage());
  }

  private LocalBucket bucket(final String type) {
    return bucket(bucketId(type));
  }

  private LocalBucket bucket(final int bucketId) {
    return (LocalBucket) database.getSchema().getBucketById(bucketId);
  }

  private int bucketId(final String type) {
    return database.getSchema().getType(type).getBuckets(false).stream().mapToInt(Bucket::getFileId).findFirst().orElseThrow();
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
