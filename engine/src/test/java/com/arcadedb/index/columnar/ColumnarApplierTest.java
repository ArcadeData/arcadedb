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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.IntHashSet;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Task 15: the apply pass re-reads dirty pages into the overlay, follows owners through the content owner maps, skips no-ops. */
class ColumnarApplierTest extends BucketPageLayoutTestSupport {
  private KillPoint killPoint;

  @Override
  protected void endTest() {
    if (killPoint != null)
      killPoint.disarm();
  }

  @Test
  void insertsAppearInTheProjection() throws Exception {
    createAsset();
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Asset").set("brand", i < 6 ? "a" : "b").save();
    });
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("Asset").set("brand", "c").save();
    });
    database.transaction(() -> database.newDocument("Asset").save());

    caughtUp();

    assertThat(brandCounts()).isEqualTo(Map.of("a", 6L, "b", 4L, "c", 3L));
    assertThat(projection().overlay().rows()).as("a record without a brand is a row too").isEqualTo(14);
  }

  @Test
  void aSecondPassOnAPageKeepsItsEarlierRows() throws Exception {
    createAsset();
    final RID first = insert("a");
    caughtUp();
    final RID second = insert("b");
    caughtUp();

    assertThat(pageOf(first)).isEqualTo(pageOf(second));
    assertThat(brandCounts()).isEqualTo(Map.of("a", 1L, "b", 1L));
    assertThat(projection().overlay().rows()).isEqualTo(2);
  }

  @Test
  void updateReplacesTheLiveValue() throws Exception {
    createAsset();
    final RID r1 = insert("a");
    insert("a");
    insert("a");
    caughtUp();
    manager().flushNow();
    assertThat(projection().overlay().rows()).isZero();

    // the old value is in a segment: its row dies (V-M18)
    setBrand(r1, "b");
    caughtUp();
    assertThat(brandCounts()).isEqualTo(Map.of("a", 2L, "b", 1L));

    // the old value is in the overlay: the row is replaced
    setBrand(r1, "c");
    caughtUp();
    assertThat(brandCounts()).isEqualTo(Map.of("a", 2L, "c", 1L));
    assertThat(projection().overlay().rows()).isEqualTo(1);

    setBrand(r1, "a");
    caughtUp();
    assertThat(brandCounts()).isEqualTo(Map.of("a", 3L));
  }

  @Test
  void updateOfAFrozenRowSupersedesIt() throws Exception {
    createAsset();
    // no background flush: the test's own flush is the one stopped at the kill point
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_FLUSH_INTERVAL, TimeUnit.HOURS.toMillis(1));
    final RID r1 = insert("a");
    insert("a");
    caughtUp();

    killPoint = KillPoint.arm(ColumnarFlusher.FLUSH_SEGMENT_WRITTEN);
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      final Future<?> flush = executor.submit(() -> {
        manager().flushNow();
        return null;
      });
      assertThat(killPoint.awaitReached(10_000)).isTrue();
      assertThat(projection().hasFrozenOverlay()).isTrue();

      setBrand(r1, "b");
      caughtUp();
      assertThat(brandCounts()).as("the frozen row of r1 is hidden by its newer value").isEqualTo(Map.of("a", 1L, "b", 1L));

      killPoint.releaseAsCrashed();
      assertThatThrownBy(flush::get).isInstanceOf(ExecutionException.class).hasCauseInstanceOf(ColumnarTestHooks.SimulatedCrash.class);
    } finally {
      killPoint.disarm();
      executor.shutdownNow();
    }

    // the retried flush writes the frozen rows with r1 dead
    manager().flushNow();
    assertThat(projection().hasFrozenOverlay()).isFalse();
    assertThat(brandCounts()).isEqualTo(Map.of("a", 1L, "b", 1L));
  }

  @Test
  void deleteRemovesThePosition() throws Exception {
    createAsset();
    final RID r1 = insert("a");
    insert("a");
    insert("a");
    caughtUp();
    manager().flushNow();
    final RID r4 = insert("b");
    caughtUp();
    assertThat(brandCounts()).isEqualTo(Map.of("a", 3L, "b", 1L));

    // r1 lives in a segment, r4 in the overlay
    database.transaction(() -> {
      r1.asDocument(true).delete();
      r4.asDocument(true).delete();
    });
    caughtUp();

    assertThat(brandCounts()).isEqualTo(Map.of("a", 2L));
    assertThat(projection().overlay().rows()).isZero();
  }

  @Test
  void slotReuseAfterDeleteIsAnOverwrite() throws Exception {
    createAsset();
    final RID r1 = insert("a");
    insert("a");
    caughtUp();
    manager().flushNow();

    final LocalBucket bucket = bucket();
    // the delete and the reuse reach one pass: the pass sees a new value at the position, never the EMPTY slot
    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> r1.asDocument(true).delete());
      database.transaction(() -> {
        final MutableDocument shell = database.newDocument("Asset").set("brand", "z");
        assertThat(TestHelper.restoreRecordAtLowLevel(database, bucket, r1.getPosition(), shell)).isEqualTo(r1);
      });
    }
    caughtUp();

    assertThat(brandCounts()).isEqualTo(Map.of("a", 1L, "z", 1L));
  }

  @Test
  void recordGrowingIntoPlaceholderIsFollowed() throws Exception {
    createAsset();
    final RID owner = insert("a");
    final long contentPage = placeholder(owner);
    caughtUp();
    assertThat(brandCounts()).isEqualTo(Map.of("a", 1L));
    assertThat(projection().owners().ownsExactly(owner.getPosition(), new int[] { (int) contentPage }, 1)).isTrue();
    manager().flushNow();

    final long resolved = stats().placeholdersResolvedCount("Asset");
    final int ownersOfContentPage = projection().owners().ownerCountUpperBound((int) contentPage);
    final IntHashSet marked;
    try (final AutoCloseable hold = manager().suspendApply()) {
      setBrand(owner, "b");
      marked = pending(projection().tracker());
    }
    assertThat(marked.contains((int) contentPage)).as("the brand lives on the content page").isTrue();
    assertThat(marked.contains(pageOf(owner))).as("the pointer slot is untouched").isFalse();
    caughtUp();

    assertThat(brandCounts()).isEqualTo(Map.of("b", 1L));
    assertThat(stats().placeholdersResolvedCount("Asset")).as("every owner of the content page was re-read through the owner map")
        .isEqualTo(resolved + ownersOfContentPage);
  }

  @Test
  void chunkedRecordUpdateOnTailChunkIsFollowed() throws Exception {
    createAsset();
    // the brand is serialized last, so its bytes end the last chunk of the chain
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Asset").set("v", "h".repeat(200 * 1024)).set("brand", "a").save().getIdentity());
    caughtUp();
    manager().flushNow();
    final int[] chunkPages = offPagePages(rid[0]);
    assertThat(chunkPages.length).isGreaterThanOrEqualTo(2);
    assertThat(projection().owners().ownsExactly(rid[0].getPosition(), chunkPages, chunkPages.length)).isTrue();

    // an update rewrites the head chunk too, so the tail-only change (a commit that rewrote the record only past its head, as the
    // commit-time merge allows) is made on the last chunk's page directly
    final RID lastChunk = lastChunkOf(rid[0]);
    final long resolved = stats().placeholdersResolvedCount("Asset");
    final IntHashSet marked;
    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> onSlot(lastChunk, page -> {
        final int offset = recordOffsetOf(page, lastChunk);
        page.writeByte(offset + CHUNK_HEADER + page.readInt(offset + 1) - 1, (byte) 'b');
        return 0L;
      }));
      marked = pending(projection().tracker());
    }
    assertThat(marked.contains(pageOf(lastChunk))).isTrue();
    assertThat(marked.contains(pageOf(rid[0]))).as("only the tail chunk changed").isFalse();
    assertThat(brandOf(rid[0])).as("the fixture changed the brand").isEqualTo("b");
    caughtUp();

    assertThat(brandCounts()).isEqualTo(Map.of("b", 1L));
    assertThat(stats().placeholdersResolvedCount("Asset")).as("the owner was found through the owner map").isEqualTo(resolved + 1);
  }

  @Test
  void recordShrinkingBackDropsStaleOwnerMapping() throws Exception {
    createAsset();
    final RID owner = insert("a");
    final int contentPage = (int) placeholder(owner);
    caughtUp();
    assertThat(projection().owners().ownsExactly(owner.getPosition(), new int[] { contentPage }, 1)).isTrue();

    // the owner alone on its page, then a growth past the content page: the slot becomes the head of a chunk chain, and as the
    // page's last record its head chunk takes the whole free tail
    database.transaction(() -> database.command("sql", "DELETE FROM Asset WHERE @rid <> " + owner));
    database.transaction(() -> owner.asDocument(true).modify().set("v", "c".repeat(200 * 1024)).save());
    assertThat((Long) bucketStats(database, "Asset").get("totalPlaceholderRecords")).isZero();
    caughtUp();
    final int[] chainPages = offPagePages(owner);
    assertThat(chainPages.length).isGreaterThanOrEqualTo(2);
    assertThat(projection().owners().ownsExactly(owner.getPosition(), chainPages, chainPages.length)).isTrue();

    // shrinking back inside the head chunk's region collapses the chain into a plain record (#6178)
    database.transaction(() -> owner.asDocument(true).modify().set("v", "p").save());
    assertThat((Long) bucketStats(database, "Asset").get("totalMultiPageRecords")).as("the chain collapsed").isZero();
    caughtUp();
    assertThat(offPagePages(owner)).isEmpty();
    assertThat(projection().owners().ownsExactly(owner.getPosition(), new int[0], 0)).as("a plain record owns no content page").isTrue();
    assertThat(projection().owners().ownerCountUpperBound(contentPage)).isZero();
    for (final int page : chainPages)
      assertThat(projection().owners().ownerCountUpperBound(page)).isZero();

    // a later write on the old content page (its commit's mark: the allocator would put a new record on the owner's emptier
    // page) re-reads no owner
    final long resolved = stats().placeholdersResolvedCount("Asset");
    final long applied = stats().applyPagesCount("Asset");
    manager().markPages(bucketId(), new int[] { contentPage }, 1);
    caughtUp();
    assertThat(stats().applyPagesCount("Asset")).as("the content page was applied").isEqualTo(applied + 1);
    assertThat(stats().placeholdersResolvedCount("Asset")).isEqualTo(resolved);
    assertThat(brandCounts()).isEqualTo(Map.of("a", 1L));
  }

  @Test
  void externalPropertyChangeIsFollowed() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("Blob", 1);
    type.createProperty("brand", Type.STRING).setExternal(true);
    database.command("sql", "CREATE INDEX ON Blob (brand) COLUMNAR");
    final int bucketId = type.getBuckets(false).getFirst().getFileId();
    final int externalId = ((LocalDocumentType) type).getExternalBucketIdFor(bucketId);
    final String before = "x".repeat(300);
    final String after = "x".repeat(299) + "y";
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Blob").set("brand", before).save().getIdentity());
    final ColumnarProjection projection = manager().projection(bucketId);
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
    assertThat(brandCounts(projection)).isEqualTo(Map.of(before, 1L));
    assertThat(projection.externalOwners().isEmpty()).as("the record owns its external value's page").isFalse();
    manager().flushNow();

    // an update rewrites the primary record too, so the change of the external value alone (its last byte) is made on the external
    // page directly
    final RID[] external = new RID[1];
    database.transaction(() -> external[0] = ((DatabaseInternal) database).getSerializer()
        .findExistingExternalRids(database, rid[0].asDocument(true)).get("brand"));
    assertThat(external[0].getBucketId()).isEqualTo(externalId);
    final long resolved = stats().placeholdersResolvedCount("Blob");
    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> onSlot(external[0], page -> {
        final int offset = recordOffsetOf(page, external[0]);
        final long[] size = page.readNumberAndSize(offset);
        page.writeByte(offset + (int) size[1] + (int) size[0] - 1, (byte) 'y');
        return 0L;
      }));
      assertThat(pending(projection.tracker()).isEmpty()).as("the primary page is untouched").isTrue();
      assertThat(pending(projection.externalTracker()).isEmpty()).isFalse();
    }
    final String[] read = new String[1];
    database.transaction(() -> read[0] = rid[0].asDocument(true).getString("brand"));
    assertThat(read[0]).as("the fixture changed the value").isEqualTo(after);
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();

    assertThat(brandCounts(projection)).isEqualTo(Map.of(after, 1L));
    assertThat(stats().placeholdersResolvedCount("Blob")).isEqualTo(resolved + 1);
    assertThat(projection.externalFileId()).isEqualTo(externalId);
  }

  @Test
  void edgeOnVertexIsANoOp() throws Exception {
    database.command("sql", "CREATE VERTEX TYPE Product BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Product.brand STRING");
    database.command("sql", "CREATE INDEX ON Product (brand) COLUMNAR");
    database.command("sql", "CREATE VERTEX TYPE Shop");
    database.command("sql", "CREATE EDGE TYPE Sells");
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newVertex("Product").set("brand", "acme").save().getIdentity();
      rids[1] = database.newVertex("Shop").save().getIdentity();
    });
    final ColumnarProjection projection = manager().projection(
        database.getSchema().getType("Product").getBuckets(false).getFirst().getFileId());
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
    manager().flushNow();
    assertThat(projection.overlay().rows()).isZero();
    final long pagesBefore = stats().applyPagesCount("Product");

    database.transaction(() -> {
      final Vertex shop = rids[1].asVertex();
      final MutableVertex product = rids[0].asVertex(true).modify();
      for (int i = 0; i < 100; i++)
        product.newEdge("Sells", shop);
    });
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();

    assertThat(stats().applyPagesCount("Product")).as("the edges rewrote the vertex page").isGreaterThan(pagesBefore);
    assertThat(projection.overlay().rows()).as("the vertex's brand did not change: no row").isZero();
    assertThat(stats().overlayRows("Product")).isZero();
    assertThat(brandCounts(projection)).isEqualTo(Map.of("acme", 1L));
  }

  @Test
  void unreadableChainKeepsPreviousValue() throws Exception {
    createAsset();
    manager().setClockForTesting(() -> 1_000_000L);
    final RID rid = chunkedRecord("a");
    caughtUp();
    final int[] chainPages = offPagePages(rid);

    final long original = breakChain(rid);
    try {
      caughtUp();
      assertThat(stats().unreadableCount("Asset")).isEqualTo(1);
      assertThat(brandCounts()).as("the previous value is still counted").isEqualTo(Map.of("a", 1L));
      assertThat(projection().owners().ownsExactly(rid.getPosition(), chainPages, chainPages.length)).as("owner map untouched").isTrue();
      assertThat(projection().unreadablePages().sizeForTesting()).isEqualTo(1);
    } finally {
      restoreChain(rid, original);
    }
    caughtUp();
    assertThat(projection().unreadablePages().sizeForTesting()).as("a readable visit ends the retries").isZero();
    assertThat(stats().unreadableCount("Asset")).isEqualTo(1);
    assertThat(projection().isUsable()).isTrue();
  }

  @Test
  void unreadablePageIsRetriedWithABackoffThenRebuilt() throws Exception {
    createAsset();
    final AtomicLong clock = new AtomicLong(1_000_000L);
    manager().setClockForTesting(clock::get);
    final List<String> rebuilds = new CopyOnWriteArrayList<>();
    projection().setRebuildRequester(rebuilds::add);
    final RID rid = chunkedRecord("a");
    caughtUp();

    final long original = breakChain(rid);
    try {
      caughtUp();
      assertThat(stats().unreadableCount("Asset")).isEqualTo(1);

      // a commit re-marking the page before the retry is due is a visit, not an attempt
      manager().markPages(bucketId(), new int[] { pageOf(rid) }, 1);
      caughtUp();
      assertThat(stats().unreadableCount("Asset")).isEqualTo(2);

      // the retry is not due: no pass re-reads the page
      Thread.sleep(300);
      assertThat(stats().unreadableCount("Asset")).isEqualTo(2);

      for (int attempt = 2; attempt <= ColumnarApplier.UNREADABLE_ATTEMPTS_BEFORE_REBUILD; attempt++) {
        assertThat(rebuilds).isEmpty();
        final long expected = stats().unreadableCount("Asset") + 1;
        clock.addAndGet(ColumnarIndexManager.MAINTENANCE_BACKOFF_MAX_MS);
        awaitUnreadable(expected);
      }
      for (int i = 0; i < 2_000 && rebuilds.isEmpty(); i++)
        Thread.sleep(5);
      assertThat(rebuilds).containsExactly(ColumnarProjection.CORRUPT);
      assertThat(projection().isUsable()).as("the previous valid state keeps serving while the rebuild runs").isTrue();
      assertThat(projection().unusableReason()).isNull();
      assertThat(brandCounts()).isEqualTo(Map.of("a", 1L));
      assertThat(projection().unreadablePages().sizeForTesting()).isZero();
    } finally {
      restoreChain(rid, original);
    }
  }

  @Test
  void anUnconvertibleValueIsCountedOncePerStagedRecord() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    // planted while the property is not in the schema: a DECIMAL a LONG column cannot hold
    final RID[] planted = new RID[1];
    database.transaction(() -> planted[0] = database.newDocument("Asset").set("brand", "a").set("fileSize", new BigDecimal("1.5")).save()
        .getIdentity());
    // the schema API, as ColumnarValueExtractorTest does: the SQL statement refuses a property its stored values break
    database.getSchema().getType("Asset").createProperty("fileSize", Type.LONG);
    database.command("sql", "CREATE INDEX ON Asset (fileSize) COLUMNAR");
    final ColumnarProjection projection = projection();
    // the create rebuild projects the record and counts its failing value once
    caughtUp();

    for (int pass = 0; pass < 4; pass++) {
      manager().markPages(bucketId(), new int[] { 0 }, 1);
      awaitDrained(projection);
    }

    long rows = 0;
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      for (final ColumnarSegmentView view : snapshot.segments())
        rows += view.live().cardinality();
    }
    assertThat(rows).as("the record is projected, its value missing").isEqualTo(1);
    assertThat(manager().conversionFailures("Asset", "fileSize")).as("re-reads of the unchanged record count nothing").isEqualTo(1);

    // a second projected column, then a change of it: the apply pass stages the record, its failing value counted with it
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    caughtUp();
    assertThat(manager().conversionFailures("Asset", "fileSize")).as("the brand rebuild counts no fileSize failure").isEqualTo(1);
    database.transaction(() -> planted[0].asDocument(true).modify().set("brand", "b").save());
    caughtUp();
    assertThat(manager().conversionFailures("Asset", "fileSize")).as("the staged record version counts its failing value").isEqualTo(2);
    for (int pass = 0; pass < 3; pass++) {
      manager().markPages(bucketId(), new int[] { 0 }, 1);
      awaitDrained(projection);
    }
    assertThat(manager().conversionFailures("Asset", "fileSize")).as("re-reads of the unchanged record count nothing").isEqualTo(2);
  }

  @Test
  void aColumnWithoutWriterDictionaryIsStagedMissing() throws Exception {
    createAsset();
    caughtUp();
    final ColumnarProjection projection = projection();
    projection.addColumn(new ColumnDefinition(999_999, "model", Type.STRING, ColumnKind.ORDINAL, false, null));
    assertThat(projection.dictionary("model")).isNull();

    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Asset").set("brand", "a").set("model", "m1").save().getIdentity());
    caughtUp();

    assertThat(stats().unreadableCount("Asset")).isZero();
    assertThat(brandCounts()).isEqualTo(Map.of("a", 1L));
    final OverlayPageBlock block = projection.overlay().page(pageOf(rid[0]));
    final int row = block.row((int) (rid[0].getPosition() % projection.maxRecordsInPage()));
    final ColumnDefinition[] columns = projection.columns();
    assertThat(columns[columns.length - 1].property()).isEqualTo("model");
    assertThat(block.present(columns.length - 1, row)).isFalse();
    assertThat(block.present(0, row)).isTrue();
  }

  @Test
  void externalOwnerFollowsAnUpdateAndADelete() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("Blob", 1);
    type.createProperty("brand", Type.STRING).setExternal(true);
    database.command("sql", "CREATE INDEX ON Blob (brand) COLUMNAR");
    final int bucketId = type.getBuckets(false).getFirst().getFileId();
    final ColumnarProjection projection = manager().projection(bucketId);
    final RID[] rid = new RID[1];
    final List<RID> neighbours = new ArrayList<>();
    database.transaction(() -> {
      rid[0] = database.newDocument("Blob").set("brand", "small").save().getIdentity();
      // neighbours filling the record's external page and the next ones: their entries must survive its update and delete
      // external pages are 256 KB (arcadedb.externalPropertyBucketDefaultPageSize)
      for (int i = 0; i < 30; i++)
        neighbours.add(database.newDocument("Blob").set("brand", "n".repeat(10_000)).save().getIdentity());
    });
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
    final int[] before = externalPagesOf(rid[0], projection);
    assertThat(before).hasSize(1);
    assertThat(projection.externalOwners().ownsExactly(rid[0].getPosition(), before, 1)).isTrue();

    // the value removed (its external record goes), then a bigger one: a new external record, where the full first page has no room
    database.transaction(() -> {
      final MutableDocument document = rid[0].asDocument(true).modify();
      document.remove("brand");
      document.save();
    });
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
    assertThat(projection.externalOwners().ownsExactly(rid[0].getPosition(), new int[0], 0)).as("no external value, no entry").isTrue();
    database.transaction(() -> rid[0].asDocument(true).modify().set("brand", "g".repeat(12_000)).save());
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
    final int[] after = externalPagesOf(rid[0], projection);
    assertThat(after).hasSize(1);
    assertThat(after[0]).as("the new external record is on another page").isNotEqualTo(before[0]);
    assertThat(projection.externalOwners().ownsExactly(rid[0].getPosition(), after, 1)).as("the entry follows the value").isTrue();
    assertThat(brandCounts(projection)).containsEntry("g".repeat(12_000), 1L);

    database.transaction(() -> rid[0].asDocument(true).delete());
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
    assertThat(projection.externalOwners().ownsExactly(rid[0].getPosition(), new int[0], 0)).as("a deleted record owns nothing").isTrue();
    for (final RID neighbour : neighbours)
      assertThat(projection.externalOwners().ownsExactly(neighbour.getPosition(), externalPagesOf(neighbour, projection), 1))
          .as("the entry of " + neighbour + " stays").isTrue();
    assertThat(brandCounts(projection)).isEqualTo(Map.of("n".repeat(10_000), 30L));
  }

  @Test
  void externalChainTailChangeIsFollowed() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("Blob", 1);
    type.createProperty("brand", Type.STRING).setExternal(true);
    database.command("sql", "CREATE INDEX ON Blob (brand) COLUMNAR");
    final ColumnarProjection projection = manager().projection(type.getBuckets(false).getFirst().getFileId());
    // longer than a 256 KB external page
    final String before = "x".repeat(600 * 1024);
    final String after = "x".repeat(600 * 1024 - 1) + "y";
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Blob").set("brand", before).save().getIdentity());
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
    manager().flushNow();
    final int[] pages = externalPagesOf(rid[0], projection);
    assertThat(pages.length).as("the external value is a chunk chain").isGreaterThanOrEqualTo(2);
    assertThat(projection.externalOwners().ownsExactly(rid[0].getPosition(), pages, pages.length)).as("every chunk page is mapped").isTrue();

    // a commit that rewrote only the last chunk of the external chain
    final RID lastChunk = lastChunkOf(externalRidOf(rid[0]));
    assertThat(lastChunk).isNotEqualTo(externalRidOf(rid[0]));
    final long resolved = stats().placeholdersResolvedCount("Blob");
    try (final AutoCloseable hold = manager().suspendApply()) {
      database.transaction(() -> onSlot(lastChunk, page -> {
        final int offset = recordOffsetOf(page, lastChunk);
        page.writeByte(offset + CHUNK_HEADER + page.readInt(offset + 1) - 1, (byte) 'y');
        return 0L;
      }));
      assertThat(pending(projection.tracker()).isEmpty()).as("the primary page is untouched").isTrue();
      final IntHashSet external = pending(projection.externalTracker());
      assertThat(external.size()).isEqualTo(1);
      assertThat(external.contains(pages[0])).as("the head page of the external value is untouched").isFalse();
    }
    final String[] read = new String[1];
    database.transaction(() -> read[0] = rid[0].asDocument(true).getString("brand"));
    assertThat(read[0]).as("the fixture changed the value").isEqualTo(after);
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();

    assertThat(brandCounts(projection)).isEqualTo(Map.of(after, 1L));
    assertThat(stats().placeholdersResolvedCount("Blob")).isEqualTo(resolved + 1);
  }

  @Test
  void passHoldsTheApplyLock() throws Exception {
    createAsset();
    caughtUp();
    final ColumnarProjection projection = projection();

    killPoint = KillPoint.arm(ColumnarApplier.BEFORE_PUBLISH);
    insert("a");
    assertThat(killPoint.awaitReached(10_000)).isTrue();
    assertThat(projection.applyLock().isLocked()).isTrue();
    assertThat(projection.applyLock().isHeldByCurrentThread()).isFalse();
    assertThat(brandCounts()).as("nothing of the pass is published before the kill point").isEmpty();

    killPoint.releaseAsCrashed();
    // the crashed pass left its page in flight: the next pass applies it
    caughtUp();
    assertThat(projection.applyLock().isLocked()).isFalse();
    assertThat(brandCounts()).isEqualTo(Map.of("a", 1L));
  }

  @Test
  void applyLagIsReadFromThePendingPagesAndClearedWhenTheyAreApplied() throws Exception {
    createAsset();
    caughtUp();
    assertThat(manager().applyLagMs("Asset")).isZero();
    // a clock one minute ahead of the tracker's: the page a pass is about to apply is a minute old
    manager().setClockForTesting(() -> System.currentTimeMillis() + 60_000);

    killPoint = KillPoint.arm(ColumnarApplier.BEFORE_PUBLISH);
    insert("a");
    assertThat(killPoint.awaitReached(10_000)).isTrue();
    assertThat(manager().applyLagMs("Asset")).isGreaterThanOrEqualTo(59_000);

    killPoint.releaseAsCrashed();
    caughtUp();
    assertThat(manager().applyLagMs("Asset")).isZero();
    assertThat(manager().dictionarySize("Asset", "brand")).isEqualTo(1);
  }

  @Test
  void aPageWaitingForAPassShowsAsLag() throws Exception {
    createAsset();
    caughtUp();
    manager().suspendApply();
    manager().setClockForTesting(() -> System.currentTimeMillis() + 60_000);
    insert("a");

    assertThat(manager().applyLagMs("Asset")).as("nothing applied it yet: the gauge must not wait for a pass").isGreaterThanOrEqualTo(59_000);
  }

  @Test
  void dictionarySizeFollowsThePassAndTheCardinalityWarningTheLimit() throws Exception {
    createAsset();
    for (final String brand : new String[] { "a", "b", "c" })
      insert(brand);
    caughtUp();
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_MAX_CARDINALITY, 3);
    assertThat(manager().dictionarySize("Asset", "brand")).isEqualTo(3);
    assertThat(manager().cardinalityExceeded("Asset", "brand")).as("at the limit is not over it").isFalse();

    insert("d");
    caughtUp();
    assertThat(manager().dictionarySize("Asset", "brand")).isEqualTo(4);
    assertThat(manager().cardinalityExceeded("Asset", "brand")).isTrue();

    // the limit is read when the gauge is, so a raise clears the warning without another publish
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_MAX_CARDINALITY, 10);
    assertThat(manager().cardinalityExceeded("Asset", "brand")).isFalse();
  }

  @Test
  void aTypeWithSeveralBucketsReportsTheWorstBucketNotTheLastPublisher() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 3");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA {\"maxCardinality\": 3}");
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_MERGE_DEAD_RATIO, 0.99f);
    final List<String> buckets = database.getSchema().getType("Asset").getBuckets(false).stream().map(Bucket::getName).toList();

    // bucket 0 holds four distinct brands (one over the limit), bucket 1 two, bucket 2 one
    final RID[] zero = new RID[4];
    for (int i = 0; i < 4; i++)
      zero[i] = insertInto(buckets.get(0), "z" + i);
    final RID[] one = { insertInto(buckets.get(1), "o0"), insertInto(buckets.get(1), "o1") };
    insertInto(buckets.get(2), "t0");
    caughtUp();
    manager().flushNow();

    assertThat(manager().dictionarySize("Asset", "brand")).as("the largest bucket dictionary, not a sum").isEqualTo(4);
    assertThat(manager().cardinalityExceeded("Asset", "brand")).isTrue();

    // another bucket publishes after the over-limit one: the warning stays
    insertInto(buckets.get(2), "t1");
    caughtUp();
    assertThat(manager().cardinalityExceeded("Asset", "brand")).isTrue();
    assertThat(manager().dictionarySize("Asset", "brand")).isEqualTo(4);

    // 8 rows flushed, 4 in bucket 0; killing 1 of bucket 0 and both of bucket 1 is 3 of 8 for the type, 1 of 4 for the first bucket
    assertThat(manager().deadRatio("Asset")).isZero();
    database.transaction(() -> {
      zero[0].asDocument(true).delete();
      one[0].asDocument(true).delete();
      one[1].asDocument(true).delete();
    });
    caughtUp();
    manager().flushNow();
    assertThat(manager().deadRatio("Asset")).isCloseTo(3.0 / 8, Offset.offset(1e-9));
  }

  @Test
  void anIndexLimitAboveTheDatabaseLimitIsNotClampedToIt() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_MAX_CARDINALITY, 3);
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA {\"maxCardinality\": 100}");
    for (final String brand : new String[] { "a", "b", "c", "d", "e" })
      insert(brand);
    caughtUp();

    assertThat(manager().dictionarySize("Asset", "brand")).as("between the database limit (3) and the index's own (100)").isEqualTo(5);
    assertThat(manager().cardinalityExceeded("Asset", "brand")).isFalse();
  }

  @Test
  void aSubtypeBucketCountsForTheParentIndex() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR METADATA {\"maxCardinality\": 3}");
    database.command("sql", "CREATE DOCUMENT TYPE Car EXTENDS Asset BUCKETS 1");
    database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_MERGE_DEAD_RATIO, 0.99f);
    insert("a");
    for (int i = 0; i < 4; i++) {
      final String brand = "c" + i;
      database.transaction(() -> database.newDocument("Car").set("brand", brand).save());
    }
    caughtUp();
    manager().flushNow();

    // only the subtype's bucket is over the limit: the parent's series must see it
    assertThat(manager().dictionarySize("Asset", "brand")).isEqualTo(4);
    assertThat(manager().cardinalityExceeded("Asset", "brand")).isTrue();

    database.transaction(() -> database.command("sql", "DELETE FROM Car WHERE brand = 'c0'"));
    caughtUp();
    manager().flushNow();
    assertThat(manager().deadRatio("Asset")).as("1 dead row of the 5 the parent index covers").isCloseTo(1.0 / 5, Offset.offset(1e-9));
  }

  // ------------------------------------------------------------------------------------------------------------------ helpers

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }

  private ColumnarStats stats() {
    return manager().getStats();
  }

  private void createAsset() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset BUCKETS 1");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
  }

  private int bucketId() {
    return database.getSchema().getType("Asset").getBuckets(false).stream().mapToInt(Bucket::getFileId).findFirst().orElseThrow();
  }

  private LocalBucket bucket() {
    return (LocalBucket) database.getSchema().getBucketById(bucketId());
  }

  private ColumnarProjection projection() {
    return manager().projection(bucketId());
  }

  private int pageOf(final RID rid) {
    return (int) (rid.getPosition() / bucket().getMaxRecordsInPage());
  }

  private void caughtUp() throws InterruptedException {
    assertThat(manager().awaitCaughtUp(10_000)).isTrue();
  }

  /** Waits until both trackers have no page in an epoch or in flight (the projection may be unusable, so not awaitCaughtUp). */
  private static void awaitDrained(final ColumnarProjection projection) throws InterruptedException {
    for (int i = 0; i < 2_000 && projection.tracker().oldestPendingEpochStartMs() != 0; i++)
      Thread.sleep(5);
    assertThat(projection.tracker().oldestPendingEpochStartMs()).isZero();
  }

  /** The external record of the record's brand value. */
  private RID externalRidOf(final RID rid) {
    final RID[] external = new RID[1];
    database.transaction(() -> external[0] = ((DatabaseInternal) database).getSerializer()
        .findExistingExternalRids(database, rid.asDocument(true)).get("brand"));
    return external[0];
  }

  /** The external-bucket pages of the record's brand value: its own, then those of its chunks when it is a chain. */
  private int[] externalPagesOf(final RID rid, final ColumnarProjection projection) {
    final RID external = externalRidOf(rid);
    assertThat(external.getBucketId()).isEqualTo(projection.externalFileId());
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(external.getBucketId());
    final int head = (int) (external.getPosition() / bucket.getMaxRecordsInPage());
    final int[][] pages = { { head } };
    bucket.visitCommittedRecord(external.getPosition(), (position, kind, content, offPagePages, offPageCount) -> {
      final int[] all = Arrays.copyOf(new int[] { head }, 1 + offPageCount);
      System.arraycopy(offPagePages, 0, all, 1, offPageCount);
      pages[0] = Arrays.stream(all).distinct().toArray();
    });
    return pages[0];
  }

  private void awaitUnreadable(final long expected) throws InterruptedException {
    for (int i = 0; i < 2_000 && stats().unreadableCount("Asset") < expected; i++)
      Thread.sleep(5);
    assertThat(stats().unreadableCount("Asset")).isEqualTo(expected);
  }

  private RID insert(final String brand) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Asset").set("brand", brand).save().getIdentity());
    return rid[0];
  }

  private RID insertInto(final String bucketName, final String brand) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Asset").set("brand", brand).save(bucketName).getIdentity());
    return rid[0];
  }

  private void setBrand(final RID rid, final String brand) {
    database.transaction(() -> rid.asDocument(true).modify().set("brand", brand).save());
  }

  /** Seals page 0 and grows the record into a placeholder; returns its content page. */
  private long placeholder(final RID rid) {
    sealFirstPage(database, "Asset");
    database.transaction(() -> rid.asDocument(true).modify().set("v", "b".repeat(20 * 1024)).save());
    assertThat((Long) bucketStats(database, "Asset").get("totalPlaceholderRecords")).as("the fixture must be a placeholder").isEqualTo(1L);
    final int[] pages = offPagePages(rid);
    assertThat(pages).hasSize(1);
    assertThat(pages[0]).isNotEqualTo(pageOf(rid));
    return pages[0];
  }

  private RID chunkedRecord(final String brand) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Asset").set("brand", brand).set("v", "h".repeat(200 * 1024)).save().getIdentity());
    return rid[0];
  }

  /** Points the head chunk past the end of the bucket ([marker:1][chunkSize:int][nextChunkPointer:long]); returns the old pointer. */
  private long breakChain(final RID rid) {
    final long[] original = new long[1];
    database.transaction(() -> original[0] = onSlot(rid, page -> {
      final int pointerOffset = recordOffsetOf(page, rid) + 1 + Binary.INT_SERIALIZED_SIZE;
      final long previous = page.readLong(pointerOffset);
      page.writeLong(pointerOffset, Integer.MAX_VALUE);
      return previous;
    }));
    return original[0];
  }

  private void restoreChain(final RID rid, final long original) {
    database.transaction(() -> onSlot(rid, page -> {
      page.writeLong(recordOffsetOf(page, rid) + 1 + Binary.INT_SERIALIZED_SIZE, original);
      return 0L;
    }));
  }

  /** [marker:1][chunkSize:int][nextChunkPointer:long], then the chunk's content. */
  private static final int CHUNK_HEADER = 1 + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

  /** Walks the chain from its head over the committed pages, read only (a page taken for modification would be marked). */
  private RID lastChunkOf(final RID head) throws IOException {
    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(head.getBucketId());
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucket.getFileId())).getPageSize();
    RID current = head;
    while (true) {
      final int slot = (int) (current.getPosition() % bucket.getMaxRecordsInPage());
      final int pageNumber = (int) (current.getPosition() / bucket.getMaxRecordsInPage());
      final ImmutablePage page = db.getPageManager().getImmutablePage(new PageId(db, bucket.getFileId(), pageNumber), pageSize, false, false);
      final int offset = (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE);
      final long next = page.readLong(offset + 1 + Binary.INT_SERIALIZED_SIZE);
      if (next == 0)
        return current;
      current = new RID(head.getBucketId(), next);
    }
  }

  private String brandOf(final RID rid) {
    final String[] brand = new String[1];
    database.transaction(() -> brand[0] = rid.asDocument(true).getString("brand"));
    return brand[0];
  }

  /** The pages holding the record's placeholder target or chunks after the head, as the committed read reports them. */
  private int[] offPagePages(final RID rid) {
    final int[][] pages = { new int[0] };
    bucket().visitCommittedRecord(rid.getPosition(), (position, kind, content, offPagePages, offPageCount) -> {
      assertThat(content).isNotNull();
      pages[0] = Arrays.copyOf(offPagePages, offPageCount);
    });
    return pages[0];
  }

  private static IntHashSet pending(final DirtyPageTracker tracker) {
    final IntHashSet pages = new IntHashSet();
    tracker.collectRecoverySet(pages);
    return pages;
  }

  private Map<Object, Long> brandCounts() {
    return brandCounts(projection());
  }

  /** The brand counts the projection answers, asserting one live value per position (spec 5.2). */
  private static Map<Object, Long> brandCounts(final ColumnarProjection projection) {
    final Map<Object, Long> counts = new TreeMap<>();
    final Set<Long> positions = new HashSet<>();
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      final GlobalDictionary dictionary = snapshot.dictionary("brand");
      for (final ColumnarSegmentView view : snapshot.segments()) {
        final PositionBitmap all = new PositionBitmap(projection.maxRecordsInPage());
        for (final int page : view.live().pages())
          all.setPage(page);
        final ColumnReader reader = view.column("brand");
        if (reader != null)
          reader.forEachOrdinal(all, (position, ordinal) -> {
            assertThat(positions.add(position)).as("position " + position + " counted twice").isTrue();
            counts.merge(dictionary.valueOf(ordinal), 1L, Long::sum);
          });
      }
    }
    return counts;
  }
}
