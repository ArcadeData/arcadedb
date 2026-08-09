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
package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5703: an {@code LSM_TREE} index used to spend a fixed 12 bytes per {@code LINK} key column
 * ({@code TYPE_RID} = {@code putInt(bucketId)} + {@code putLong(position)}), while the very same RID costs 2-7
 * bytes in the varint {@code TYPE_COMPRESSED_RID} form the index already uses for every entry <i>value</i>.
 * On the composite {@code (@out,@in)} key that motivates an endpoint-keyed unique index that is 24 bytes of key
 * against a whole-entry cost of roughly 37, so it dominates the page.
 * <p>
 * The narrower encoding is safe for an ORDERED index only because the LSM tree never compares raw key bytes: it
 * deserializes both sides and compares typed values ({@code LSMTreeIndexAbstract.compareKey}), so key order comes
 * from {@link RID#compareTo} and not from the byte layout. These tests pin that down - order, range bounds and
 * lookups over LINK keys must be identical to what the fixed-width encoding produced - plus the two traps the
 * sibling HASH fix (#5677) already hit:
 * <ul>
 *   <li>the type reported OUTWARD must stay the schema type ({@code TYPE_RID}), otherwise coercing
 *       {@code WHERE link = "#150:5"} into a RID breaks with a {@code ClassCastException};</li>
 *   <li>an index already on disk must keep the encoding ITS OWN page header declares, so a database written
 *       before this change keeps reading its 12-byte keys.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5703LsmLinkKeyCompressedRidTest extends TestHelper {
  /** Bytes a single fixed-width LINK column costs on the page: 1 null flag + putInt(bucketId) + putLong(position). */
  private static final int LEGACY_BYTES_PER_LINK_COLUMN = 1 + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

  @Test
  void linkKeyIsStoredCompressedYetStillReportedAsRid() {
    createEndpointIndexedGraph(64);

    final LSMTreeIndexMutable mutable = firstPopulatedMutableIndex("INITIATED[@out,@in]");

    // ON THE PAGE: the varint form
    assertThat(mutable.storageKeyTypes).containsExactly(BinaryTypes.TYPE_COMPRESSED_RID, BinaryTypes.TYPE_COMPRESSED_RID);
    // OUTWARD: still exactly what the schema declares, so key coercion keeps working (the #5677 trap)
    assertThat(mutable.getBinaryKeyTypes()).containsExactly(BinaryTypes.TYPE_RID, BinaryTypes.TYPE_RID);
    assertThat(mutable.getKeyTypes()).containsExactly(Type.LINK, Type.LINK);

    // ...and the bytes really are narrower on the page, which is the whole point of the issue.
    database.transaction(() -> assertThat(firstEntryKeySize(mutable)).isLessThan(2 * LEGACY_BYTES_PER_LINK_COLUMN));
  }

  @Test
  void orderedIterationOverLinkKeysFollowsRidOrder() {
    final List<RID> targets = createLinkIndexedDocuments(400);

    final List<RID> sorted = new ArrayList<>(targets);
    Collections.sort(sorted);

    final RangeIndex index = (RangeIndex) singleBucketIndex("Doc[ref]");

    database.transaction(() -> {
      assertThat(keysOf(index.iterator(true))).containsExactlyElementsOf(sorted);

      final List<RID> descending = new ArrayList<>(sorted);
      Collections.reverse(descending);
      assertThat(keysOf(index.iterator(false))).containsExactlyElementsOf(descending);

      // A BOUNDED RANGE must select exactly the same slice the fixed-width encoding selected.
      final RID from = sorted.get(50);
      final RID to = sorted.get(150);
      assertThat(keysOf(index.range(true, new Object[] { from }, true, new Object[] { to }, true)))
          .containsExactlyElementsOf(sorted.subList(50, 151));

      // ...and the exclusive bounds must drop exactly the two endpoints.
      assertThat(keysOf(index.range(true, new Object[] { from }, false, new Object[] { to }, false)))
          .containsExactlyElementsOf(sorted.subList(51, 150));
    });
  }

  @Test
  void everyLinkKeyIsFoundByAnExactLookup() {
    final List<RID> targets = createLinkIndexedDocuments(400);
    final Index index = database.getSchema().getIndexByName("Doc[ref]");

    database.transaction(() -> {
      for (final RID target : targets)
        try (final IndexCursor cursor = index.get(new Object[] { target })) {
          assertThat(cursor.hasNext()).as("lookup of %s", target).isTrue();
          assertThat(cursor.next().getRecord().asDocument().get("ref")).isEqualTo(target);
        }
    });
  }

  @Test
  void compactionPreservesLinkKeyOrderAndLookups() throws Exception {
    // A small index page so a few thousand entries really do span several pages: LSMTreeIndexCompactor refuses to
    // run below 2, and compact() itself returns false unless a compaction was scheduled first - so without both of
    // these the "after compaction" assertions below would never see a compacted sub-index at all.
    final List<RID> targets = createLinkIndexedDocuments(4_000, 8192);

    final LSMTreeIndex lsmIndex = (LSMTreeIndex) singleBucketIndex("Doc[ref]");
    assertThat(lsmIndex.getMutableIndex().getTotalPages()).as("mutable pages before compaction").isGreaterThan(1);
    assertThat(lsmIndex.getMutableIndex().getSubIndex()).as("not compacted yet").isNull();

    assertThat(lsmIndex.scheduleCompaction()).isTrue();
    assertThat(lsmIndex.compact()).as("compaction ran").isTrue();

    final LSMTreeIndex compacted = (LSMTreeIndex) singleBucketIndex("Doc[ref]");
    assertThat(compacted.getMutableIndex().getSubIndex()).as("compacted sub-index").isNotNull();

    final List<RID> sorted = new ArrayList<>(targets);
    Collections.sort(sorted);

    database.transaction(() -> {
      assertThat(keysOf(compacted.iterator(true))).containsExactlyElementsOf(sorted);
      for (final RID target : targets)
        try (final IndexCursor cursor = compacted.get(new Object[] { target })) {
          assertThat(cursor.hasNext()).as("lookup of %s after compaction", target).isTrue();
        }
    });
  }

  @Test
  void compositeEndpointUniqueIndexStillRejectsDuplicates() {
    createEndpointIndexedGraph(16);

    final RID[] endpoints = new RID[2];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Account").set("code", "HUB2").save();
      final MutableVertex leaf = database.newVertex("Account").set("code", "LEAF2").save();
      hub.newEdge("INITIATED", leaf).save();
      endpoints[0] = hub.getIdentity();
      endpoints[1] = leaf.getIdentity();
    });

    assertThatThrownBy(() -> database.transaction(() -> database.lookupByRID(endpoints[0], true).asVertex().modify()
        .newEdge("INITIATED", database.lookupByRID(endpoints[1], true).asVertex()).save()))
        .isInstanceOf(DuplicatedKeyException.class);

    final Index index = database.getSchema().getIndexByName("INITIATED[@out,@in]");
    database.transaction(() -> {
      try (final IndexCursor found = index.get(new Object[] { endpoints[0], endpoints[1] })) {
        assertThat(found.hasNext()).isTrue();
      }
      // THE REVERSED PAIR IS A DIFFERENT KEY: the composite order must survive the narrower encoding.
      try (final IndexCursor reversed = index.get(new Object[] { endpoints[1], endpoints[0] })) {
        assertThat(reversed.hasNext()).isFalse();
      }
    });
  }

  @Test
  void aLinkKeyIsStillCoercedFromItsStringForm() {
    final List<RID> targets = createLinkIndexedDocuments(8);
    final RID target = targets.getFirst();

    // #5677's regression in the sibling implementation: reporting the STORAGE type outward made the engine
    // coerce "#1:0" against TYPE_COMPRESSED_RID, whose class mapping is undefined, and throw.
    try (final ResultSet result = database.query("sql", "SELECT FROM Doc WHERE ref = ?", target.toString())) {
      assertThat(result.stream().count()).isEqualTo(1);
    }
  }

  @Test
  void anIndexWrittenWithFixedWidthKeysKeepsUsingThem() throws Exception {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc", 1);
      database.getSchema().createVertexType("Target", 1);
      database.getSchema().getType("Doc").createProperty("ref", Type.LINK);
      database.getSchema().buildTypeIndex("Doc", new String[] { "ref" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    });

    final String fileName = ((LSMTreeIndex) singleBucketIndex("Doc[ref]")).getMutableIndex().getName();
    database.close();

    // Rewrite the key-type byte page 0 declares back to the pre-#5703 fixed-width type, which is exactly the
    // state a database created by an older engine is in. Asserting the current value first keeps the test
    // honest if the page-0 layout ever moves.
    downgradeFirstKeyTypeOnPageZero(fileName);

    database = factory.open();

    final LSMTreeIndexMutable mutable = ((LSMTreeIndex) singleBucketIndex("Doc[ref]")).getMutableIndex();
    assertThat(mutable.storageKeyTypes).containsExactly(BinaryTypes.TYPE_RID);
    assertThat(mutable.getBinaryKeyTypes()).containsExactly(BinaryTypes.TYPE_RID);
    assertThat(mutable.getKeyTypes()).containsExactly(Type.LINK);

    final List<RID> targets = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final RID target = database.newVertex("Target").set("id", i).save().getIdentity();
        targets.add(target);
        database.newDocument("Doc").set("ref", target).save();
      }
    });

    final List<RID> sorted = new ArrayList<>(targets);
    Collections.sort(sorted);

    final RangeIndex index = (RangeIndex) singleBucketIndex("Doc[ref]");
    database.transaction(() -> {
      assertThat(keysOf(index.iterator(true))).containsExactlyElementsOf(sorted);
      // AND THE KEYS REALLY ARE THE WIDE ONES: one column, fixed 12 bytes plus the null flag.
      assertThat(firstEntryKeySize(mutable)).isEqualTo(LEGACY_BYTES_PER_LINK_COLUMN);
    });
  }

  // ---------------------------------------------------------------------------------------------------------

  private void createEndpointIndexedGraph(final int edges) {
    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      database.getSchema().createEdgeType("INITIATED");
      database.getSchema().buildTypeIndex("INITIATED", new String[] { "@out", "@in" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
    });

    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Account").set("code", "HUB").save();
      for (int i = 0; i < edges; i++)
        hub.newEdge("INITIATED", database.newVertex("Account").set("code", "L" + i).save()).save();
    });
  }

  /**
   * Creates {@code count} documents, each linking a distinct vertex, indexed by an LSM index on the LINK property.
   * Both types get a single bucket so the index has exactly one underlying LSM tree and its iteration order is the
   * global one, with no cursor merge in between.
   */
  private List<RID> createLinkIndexedDocuments(final int count) {
    return createLinkIndexedDocuments(count, 0);
  }

  private List<RID> createLinkIndexedDocuments(final int count, final int indexPageSize) {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc", 1);
      database.getSchema().createVertexType("Target", 1);
      database.getSchema().getType("Doc").createProperty("ref", Type.LINK);
      final var builder = database.getSchema().buildTypeIndex("Doc", new String[] { "ref" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false);
      if (indexPageSize > 0)
        builder.withPageSize(indexPageSize);
      builder.create();
    });

    final List<RID> targets = new ArrayList<>(count);
    database.transaction(() -> {
      // Positions climb past the 1-, 2- and 3-byte varint widths, so the encoding under test is exercised at the
      // widths where a byte-order-based comparison would diverge from RID order.
      for (int i = 0; i < count; i++) {
        final RID target = database.newVertex("Target").set("id", i).save().getIdentity();
        targets.add(target);
        database.newDocument("Doc").set("ref", target).save();
      }
    });
    return targets;
  }

  private IndexInternal singleBucketIndex(final String indexName) {
    final IndexInternal[] subIndexes = ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets();
    assertThat(subIndexes).hasSize(1);
    return subIndexes[0];
  }

  private LSMTreeIndexMutable firstPopulatedMutableIndex(final String indexName) {
    for (final IndexInternal sub : ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets())
      if (sub.countEntries() > 0)
        return ((LSMTreeIndex) sub).getMutableIndex();
    throw new AssertionError("No populated underlying index found for '" + indexName + "'");
  }

  private static List<RID> keysOf(final IndexCursor cursor) {
    final List<RID> keys = new ArrayList<>();
    try (cursor) {
      while (cursor.hasNext()) {
        cursor.next();
        keys.add((RID) cursor.getKeys()[0]);
      }
    }
    return keys;
  }

  /** Bytes the FIRST entry of page 0 spends on its key, read back through the index's own key reader. */
  private int firstEntryKeySize(final LSMTreeIndexMutable mutable) {
    try {
      final BasePage page = ((DatabaseInternal) database).getTransaction()
          .getPage(new PageId(database, mutable.getFileId(), 0), mutable.getPageSize());
      final Binary buffer = new Binary(page.slice());
      final int startIndexArray = mutable.getHeaderSize(0);
      assertThat(mutable.getCount(page)).isPositive();
      buffer.position(buffer.getInt(startIndexArray));
      return mutable.getSerializedKeySize(buffer, mutable.getBinaryKeyTypes().length);
    } catch (final IOException e) {
      throw new IllegalStateException("Cannot read page 0 of index " + mutable.getName(), e);
    }
  }

  /**
   * Flips the first key-type byte of page 0 from the compressed form back to the fixed-width one, simulating an
   * index file written before #5703. Page 0's layout is
   * {@code [pageHeader][int][int][byte][int][subIndexFileId:int][numKeys:byte][keyType:byte]*}.
   */
  private void downgradeFirstKeyTypeOnPageZero(final String fileName) throws Exception {
    final File[] candidates = new File(getDatabasePath()).listFiles((dir, name) -> name.startsWith(fileName + "."));
    assertThat(candidates).isNotNull().hasSize(1);

    final int keyTypesOffset = BasePage.PAGE_HEADER_SIZE //
        + Binary.INT_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE + Binary.BYTE_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE //
        + Binary.INT_SERIALIZED_SIZE  // sub-index file id
        + Binary.BYTE_SERIALIZED_SIZE; // number of key columns

    try (final RandomAccessFile file = new RandomAccessFile(candidates[0], "rw")) {
      file.seek(keyTypesOffset - Binary.BYTE_SERIALIZED_SIZE);
      assertThat(file.readByte()).as("number of key columns on page 0").isEqualTo((byte) 1);
      assertThat(file.readByte()).as("key type on page 0").isEqualTo(BinaryTypes.TYPE_COMPRESSED_RID);
      file.seek(keyTypesOffset);
      file.writeByte(BinaryTypes.TYPE_RID);
    }
  }
}
