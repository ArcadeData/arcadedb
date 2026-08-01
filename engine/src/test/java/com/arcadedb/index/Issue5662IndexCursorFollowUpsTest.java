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
package com.arcadedb.index;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Cursor;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.IndexCursorCollection;
import com.arcadedb.database.RID;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #5662, the follow-ups left open by #5635.
 * <p>
 * The item this file spends most of its length on is the first one: nothing made an unclosed {@link IndexCursor}
 * visible. {@link Cursor} is now {@link AutoCloseable}, which is what makes an abandoned cursor a static-analysis
 * finding instead of something only a careful read can catch - but a compiler flag proves nothing at runtime, so the
 * tests here pin the two things that do: the call sites that were abandoning a cursor partway now release it, and a
 * cursor that is leaked anyway stops pinning its file forever once the GC gets to it.
 * <p>
 * The observable throughout is {@link LSMTreeIndexCompacted#countActiveCursors()}, the guard
 * {@code LSMTreeIndex.dropRetiredCompactedIndexes} consults before physically dropping a file a full compaction has
 * replaced. Every test that uses it first asserts it can actually rise above zero on this fixture, because a scan too
 * small to open a series cursor would make "it went back to zero" hold vacuously.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5662IndexCursorFollowUpsTest extends TestHelper {
  private static final String TYPE_NAME = "CursorFollowUp";
  private static final int    RECORDS   = 600;

  @Override
  public void beforeTest() {
    // explicit compaction only, so the sub-index appears exactly when this test asks for it
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // item 1 - nothing enforced close()
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void anIndexCursorIsUsableAsATryWithResourcesResource() throws Exception {
    final LSMTreeIndexCompacted compacted = createCompactedFixture();

    // the point of the item: this compiles, which it did not before Cursor extended AutoCloseable. close() is also
    // declared without a checked exception, so the caller is not forced into a catch(Exception) it has nothing to do
    // with
    try (final IndexCursor cursor = bucketIndex().iterator(true)) {
      assertThat(cursor.hasNext()).isTrue();
      cursor.next();
      assertThat(compacted.countActiveCursors()).as("a live scan holds a series cursor over the compacted file").isPositive();
    }

    assertThat(compacted.countActiveCursors()).as("try-with-resources released the series cursors").isZero();
  }

  @Test
  void aSelectThatStopsAtTheFirstMatchReleasesItsIndexCursor() {
    final LSMTreeIndexCompacted compacted = createCompactedFixture();

    // exists() returns on the FIRST match, so the cursor is abandoned with most of the index still ahead of it: the
    // shape that used to leave a series cursor registered for the lifetime of the database
    final boolean found = database.select().fromType(TYPE_NAME).where().property("value").gt().value(1).exists();

    assertThat(found).as("the fixture must actually match, otherwise the scan never opened a cursor").isTrue();
    assertThat(compacted.countActiveCursors()).as("exists() must release the index cursor it abandoned partway").isZero();
  }

  @Test
  void aCountWithALimitReleasesItsIndexCursor() {
    final LSMTreeIndexCompacted compacted = createCompactedFixture();

    final long counted = database.select().fromType(TYPE_NAME).where().property("value").gt().value(1).limit(5).count();

    assertThat(counted).as("the limit must actually cut the scan short").isEqualTo(5);
    assertThat(compacted.countActiveCursors()).as("count() must release the index cursor it abandoned at the limit").isZero();
  }

  @Test
  void closingAPartiallyConsumedSelectIteratorReleasesItsIndexCursor() {
    final LSMTreeIndexCompacted compacted = createCompactedFixture();

    final var iterator = database.select().fromType(TYPE_NAME).where().property("value").gt().value(1).documents();
    assertThat(iterator.hasNext()).isTrue();
    iterator.next();
    assertThat(compacted.countActiveCursors()).as("the iterator is mid-scan, so a series cursor is open").isPositive();

    iterator.close();

    assertThat(compacted.countActiveCursors()).as("SelectIterator.close() must release the source index cursor").isZero();
  }

  /**
   * A {@code DELETE ... WHERE} puts the index scan in a SUB-plan and the {@code LIMIT} outside it, so the scan is
   * abandoned partway. Closing the result set used to close only the outer plan's steps - {@code SubQueryStep} never
   * closed the plan it wraps - and the scan inside it was released only if it happened to run to exhaustion.
   */
  @Test
  void closingADeleteLimitedShortOfExhaustionReleasesTheScanInsideItsSubPlan() {
    final LSMTreeIndexCompacted compacted = createCompactedFixture();

    database.transaction(() -> {
      try (final ResultSet result = database.command("sql", "DELETE FROM " + TYPE_NAME + " WHERE value > 1 LIMIT 3")) {
        assertThat(((Number) result.next().getProperty("count")).intValue()).as("the LIMIT must cut the scan short")
            .isEqualTo(3);
        assertThat(compacted.countActiveCursors()).as("precondition: the abandoned scan is still holding a series cursor")
            .isPositive();
      }

      assertThat(compacted.countActiveCursors()).as("closing the result set must reach the scan inside the sub-plan")
          .isZero();
    });
  }

  /**
   * The safety net behind the whole item: a cursor that IS leaked - because some call site nobody has read yet still
   * drops one - must stop blocking the retired-file drop once it becomes unreachable. It used to be counted, so a
   * single missed {@code close()} pinned the file with nothing left in the process that could ever release it.
   * <p>
   * This is the one test here that depends on the JVM rather than only on this codebase: it needs the collector to
   * actually clear a weak referent. That is reliable on HotSpot for a genuinely unreachable object - the cursor is
   * opened in its own frame so no local of this method keeps it on the stack - but it is a property of the runtime,
   * not a guarantee of the spec, so a different collector under load could in principle make it flake. It is asserted
   * rather than treated as best-effort on purpose: a test that silently passes when the safety net does not work
   * would be worse than one that occasionally needs a re-run.
   */
  @Test
  void aLeakedCursorStopsPinningTheFileOnceItIsCollected() {
    final LSMTreeIndexCompacted compacted = createCompactedFixture();

    leakACursorMidScan();

    assertThat(compacted.countActiveCursors()).as("the abandoned cursor is still reachable, so it still pins the file")
        .isPositive();

    // the reference the registry holds is weak, so collecting the cursor is enough to release the file
    awaitCollection(compacted);

    assertThat(compacted.countActiveCursors()).as("a collected-but-never-closed cursor must stop pinning the file").isZero();
  }

  /**
   * Opens a scan, advances it into the compacted series and drops every reference to it without closing. Kept in its
   * own frame so no local of the calling method keeps the cursor alive on the stack.
   */
  private void leakACursorMidScan() {
    final IndexCursor leaked = bucketIndex().iterator(true);
    assertThat(leaked.hasNext()).isTrue();
    leaked.next();
  }

  private void awaitCollection(final LSMTreeIndexCompacted compacted) {
    for (int attempt = 0; attempt < 50 && compacted.countActiveCursors() > 0; attempt++) {
      System.gc();
      try {
        Thread.sleep(20);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  /**
   * A {@link MultiIndexCursor} that fails to construct must not leave the children it was handed open - nothing else
   * holds a reference to them. It also NULLS their slots in the caller's list, because it keeps that list rather than
   * copying it, which is why {@code TypeIndex.range()} has to null-guard its own cleanup: without the guard it would
   * throw a {@link NullPointerException} over the exception being propagated.
   */
  @Test
  void aMultiIndexCursorThatFailsToConstructClosesTheChildrenAndClearsTheList() {
    final ProbeCountingCursor healthy = new ProbeCountingCursor(new RID(1, 1));
    final ProbeCountingCursor failing = new ProbeCountingCursor(new RID(1, 2));
    failing.failOnHasNext = true;
    final List<IndexCursor> children = new ArrayList<>(List.of(healthy, failing));

    assertThatThrownBy(() -> new MultiIndexCursor(children, -1, true))
        .as("the original failure must reach the caller, not a NullPointerException from the cleanup")
        .isInstanceOf(IllegalStateException.class).hasMessage("page read failed on purpose");

    assertThat(healthy.closeCalls).as("a child the caller can no longer reach must be closed").isOne();
    assertThat(failing.closeCalls).isOne();
    assertThat(children).as("the caller's own list is left holding nulls, hence the null guard in TypeIndex.range()")
        .containsOnlyNulls();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // item 4 - a cursor must iterate ITSELF
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void aCollectionBackedCursorIteratesItself() {
    final List<Identifiable> rids = List.of(new RID(1, 1), new RID(1, 2), new RID(1, 3));
    final IndexCursorCollection cursor = new IndexCursorCollection(rids);

    assertThat(cursor.iterator()).as("a for-each must drive the cursor, not a second independent traversal").isSameAs(cursor);

    int seen = 0;
    for (final Identifiable rid : cursor) {
      // pre-#5662 the backing iterator was handed back directly, so next() - and with it getRecord() - was bypassed
      assertThat(cursor.getRecord()).as("getRecord() must track the element the for-each just produced").isEqualTo(rid);
      ++seen;
    }
    assertThat(seen).isEqualTo(rids.size());
  }

  /**
   * The vector search cursor was the one implementation that handed back the backing list's iterator, so a for-each
   * got a SECOND, independent traversal: it never advanced the cursor's own position, and mixing it with
   * {@code next()} read some RIDs twice.
   */
  @Test
  void theVectorSearchCursorIteratesItself() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE VectorDoc");
      database.command("sql", "CREATE PROPERTY VectorDoc.name STRING");
      database.command("sql", "CREATE PROPERTY VectorDoc.embedding ARRAY_OF_FLOATS");
      database.command("sql", "CREATE INDEX ON VectorDoc (name) UNIQUE");
      database.command("sql", """
          CREATE INDEX ON VectorDoc (embedding) LSM_VECTOR
          METADATA { dimensions: 3, similarity: 'COSINE', idPropertyName: 'name' }""");
    });
    database.transaction(() -> {
      database.newDocument("VectorDoc").set("name", "a").set("embedding", new float[] { 1f, 0f, 0f }).save();
      database.newDocument("VectorDoc").set("name", "b").set("embedding", new float[] { 0f, 1f, 0f }).save();
      database.newDocument("VectorDoc").set("name", "c").set("embedding", new float[] { 0f, 0f, 1f }).save();
    });

    final Index vectorIndex = database.getSchema().getType("VectorDoc").getPolymorphicIndexByProperties("embedding")
        .getIndexesOnBuckets()[0];

    try (final IndexCursor cursor = vectorIndex.get(new Object[] { new float[] { 1f, 0f, 0f } }, 3)) {
      assertThat(cursor.iterator()).as("a for-each must drive the cursor, not a second independent traversal")
          .isSameAs(cursor);

      int seen = 0;
      for (final Identifiable rid : cursor) {
        assertThat(cursor.getRecord()).as("getRecord() must track the element the for-each just produced").isEqualTo(rid);
        ++seen;
      }
      assertThat(seen).as("the fixture must return neighbours, otherwise the loop body never ran").isPositive();
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // item 5 - the MultiIndexCursor accessors must not probe the children
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void multiIndexCursorAccessorsDoNotDriveTheChildren() {
    final ProbeCountingCursor child = new ProbeCountingCursor(new RID(1, 1), new RID(1, 2));
    final MultiIndexCursor cursor = new MultiIndexCursor(new ArrayList<>(List.of(child)), -1, true);

    final int probesAfterConstruction = child.hasNextCalls;
    assertThat(probesAfterConstruction).as("the fixture must have a child that CAN be probed").isPositive();

    cursor.getComparator();
    cursor.getBinaryKeyTypes();
    cursor.getComparator();
    cursor.getBinaryKeyTypes();

    // since #5635 hasNext() prefetches - it runs page reads and tombstone skips - so an accessor that probed the
    // children turned two plain getters into scan work
    assertThat(child.hasNextCalls).as("the accessors must answer from state sampled at construction")
        .isEqualTo(probesAfterConstruction);
  }

  @Test
  void multiIndexCursorStillReportsItsKeyTypesOnceExhausted() {
    final ProbeCountingCursor child = new ProbeCountingCursor(new RID(1, 1));
    final MultiIndexCursor cursor = new MultiIndexCursor(new ArrayList<>(List.of(child)), -1, true);

    assertThat(cursor.getBinaryKeyTypes()).as("precondition: the key types are known while the cursor is live").isNotNull();

    while (cursor.hasNext())
      cursor.next();

    // the old formulation returned the first child that still HAD something, so an exhausted cursor reported null even
    // though its key types never changed
    assertThat(cursor.getBinaryKeyTypes()).as("key types do not depend on how much of the cursor is left").isNotNull();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // fixture
  // ---------------------------------------------------------------------------------------------------------------

  private LSMTreeIndexCompacted createCompactedFixture() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("value", Integer.class);
    // a small page size so the compacted output spans several pages, i.e. a real series a cursor walks lazily
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "value" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true).withPageSize(1024).create();

    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE_NAME).set("value", i).save();
    });

    final LSMTreeIndex index = bucketIndex();
    try {
      if (index.scheduleCompaction())
        index.compact();
    } catch (final IOException | InterruptedException e) {
      throw new IllegalStateException("cannot compact the fixture index", e);
    }

    final LSMTreeIndexCompacted compacted = index.getMutableIndex().getSubIndex();
    assertThat(compacted).as("the fixture must produce a compacted sub-index, otherwise no series cursor is ever opened")
        .isNotNull();
    assertThat(compacted.countActiveCursors()).as("no scan has run yet").isZero();
    return compacted;
  }

  private LSMTreeIndex bucketIndex() {
    return (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
  }

  /** Counts how many times an accessor drove it, which since #5635 is not a free operation on a real cursor. */
  private static final class ProbeCountingCursor implements IndexCursor {
    private final List<Identifiable> values;
    private       int                position;
    private       int                hasNextCalls;
    private       int                closeCalls;
    private       boolean            failOnHasNext;

    private ProbeCountingCursor(final Identifiable... values) {
      this.values = List.of(values);
    }

    @Override
    public Object[] getKeys() {
      return new Object[] { position };
    }

    @Override
    public Identifiable getRecord() {
      return position == 0 ? null : values.get(position - 1);
    }

    @Override
    public boolean hasNext() {
      ++hasNextCalls;
      if (failOnHasNext)
        // stands in for a lazy page read failing while the cursor is being wired up
        throw new IllegalStateException("page read failed on purpose");
      return position < values.size();
    }

    @Override
    public void close() {
      ++closeCalls;
    }

    @Override
    public Identifiable next() {
      if (position >= values.size())
        throw new NoSuchElementException();
      return values.get(position++);
    }

    @Override
    public long estimateSize() {
      return values.size() - position;
    }

    @Override
    public BinaryComparator getComparator() {
      return null;
    }

    @Override
    public byte[] getBinaryKeyTypes() {
      return new byte[] { 1 };
    }

    @Override
    public Iterator<Identifiable> iterator() {
      return this;
    }
  }
}
