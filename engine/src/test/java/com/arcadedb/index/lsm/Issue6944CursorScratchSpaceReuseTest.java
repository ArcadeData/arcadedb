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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #6944: {@link LSMTreeIndexCursor#fetchNext()} allocated a fresh {@code ArrayList},
 * {@code HashMap} and {@code HashSet} for every key group it emitted, i.e. per row on a unique-index scan. The fix
 * hoists the three containers to instance fields and {@code clear()}s them at the start of every group instead.
 * <p>
 * That reuse is only safe if every group starts from a genuinely empty container - the risky case is a group with
 * NO live page cursor (every {@code pageCursors[p]} already exhausted and null), which is reached once a scan works
 * through all committed disk entries and only the in-transaction overlay tail remains: the "FIND THE MINOR KEY" loop
 * never touches {@code minorKeyIndexes} on that path, so a reused field would otherwise carry over the previous
 * group's (unrelated) cursor indices instead of the explicit {@code clear()} added at the top of the round. These
 * tests force exactly that page-exhausted-then-tx-only-tail transition, in both directions, plus a duplicate/tombstone
 * heavy scan that cycles the {@code ridState}/{@code mergedRIDs} fields across many groups.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6944CursorScratchSpaceReuseTest extends TestHelper {

  @Test
  void txOnlyTailAfterDiskExhaustionAscending() {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc6944a").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    database.getSchema().buildTypeIndex("Doc6944a", new String[] { "a" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    // Committed baseline: several disk-backed key groups.
    database.transaction(() -> {
      for (int i = 0; i < 20; ++i)
        database.newDocument("Doc6944a").set("a", i).save();
    });

    database.begin();
    try {
      // Uncommitted keys strictly ABOVE every committed key: once the scan exhausts the disk cursors it must
      // keep emitting these tx-only groups WITHOUT inheriting the last disk group's stale cursor indices.
      for (int i = 20; i < 30; ++i)
        database.newDocument("Doc6944a").set("a", i).save();

      final Set<Integer> seen = new HashSet<>();
      final ResultSet rs = database.query("sql", "SELECT a FROM Doc6944a ORDER BY a ASC");
      while (rs.hasNext())
        seen.add((Integer) rs.next().getProperty("a"));

      assertThat(seen).as("both the committed disk entries and the uncommitted tx-only tail must be visible")
          .containsExactlyInAnyOrderElementsOf(java.util.stream.IntStream.range(0, 30).boxed().toList());
    } finally {
      database.rollback();
    }
  }

  @Test
  void txOnlyTailAfterDiskExhaustionDescending() {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc6944b").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    database.getSchema().buildTypeIndex("Doc6944b", new String[] { "a" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    database.transaction(() -> {
      for (int i = 20; i < 40; ++i)
        database.newDocument("Doc6944b").set("a", i).save();
    });

    database.begin();
    try {
      // Uncommitted keys strictly BELOW every committed key: in DESC order these become the tx-only tail once
      // the disk cursors (the higher keys) are exhausted.
      for (int i = 0; i < 20; ++i)
        database.newDocument("Doc6944b").set("a", i).save();

      final Set<Integer> seen = new HashSet<>();
      final ResultSet rs = database.query("sql", "SELECT a FROM Doc6944b ORDER BY a DESC");
      while (rs.hasNext())
        seen.add((Integer) rs.next().getProperty("a"));

      assertThat(seen).as("both the committed disk entries and the uncommitted tx-only tail must be visible")
          .containsExactlyInAnyOrderElementsOf(java.util.stream.IntStream.range(0, 40).boxed().toList());
    } finally {
      database.rollback();
    }
  }

  @Test
  void manyGroupsWithDuplicatesAndTombstonesSurviveFieldReuse() {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc6944c").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    database.getSchema().buildTypeIndex("Doc6944c", new String[] { "a" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    // 3 committed batches (separate transactions -> separate mutable pages) so keys interleave across pages,
    // each key duplicated a few times, to exercise the ridState/mergedRIDs merge across many key groups.
    for (int batch = 0; batch < 3; ++batch) {
      final int b = batch;
      database.transaction(() -> {
        for (int i = 0; i < 100; ++i)
          for (int dup = 0; dup < 2; ++dup)
            database.newDocument("Doc6944c").set("a", i).set("batch", b).set("dup", dup).save();
      });
    }

    // Delete every RID belonging to duplicate slot 0 to force per-RID tombstones interleaved with the survivors.
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Doc6944c WHERE dup = 0");
      while (rs.hasNext())
        rs.next().getRecord().get().asDocument().delete();
    });

    int count = 0;
    final Set<String> distinctKeysSeen = new HashSet<>();
    final ResultSet rs = database.query("sql", "SELECT a FROM Doc6944c ORDER BY a ASC");
    while (rs.hasNext()) {
      distinctKeysSeen.add(rs.next().getProperty("a").toString());
      ++count;
    }

    // 3 batches * 1 surviving duplicate (dup=1) per key = 3 rows per key, 100 distinct keys.
    assertThat(count).as("only the non-tombstoned duplicates must survive the scan").isEqualTo(300);
    assertThat(distinctKeysSeen).hasSize(100);
  }

  /**
   * The actual perf claim: {@code minorKeyIndexes}/{@code ridState}/{@code mergedRIDs} must be the SAME container
   * instance across key groups, not a fresh {@code ArrayList}/{@code HashMap}/{@code HashSet} per group. Before the
   * fix, this identity check fails on the very first two rows.
   */
  @Test
  void scratchContainersAreReusedAcrossGroups() throws Exception {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc6944d").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    database.getSchema().buildTypeIndex("Doc6944d", new String[] { "a" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    database.transaction(() -> {
      for (int i = 0; i < 10; ++i)
        database.newDocument("Doc6944d").set("a", i).save();
    });

    database.transaction(() -> {
      try {
        final var typeIndex = database.getSchema().getType("Doc6944d").getIndexesByProperties("a").getFirst();
        LSMTreeIndexMutable mutable = null;
        for (final var bucketIndex : typeIndex.getIndexesOnBuckets())
          if (bucketIndex instanceof LSMTreeIndex lsmIndex)
            mutable = lsmIndex.getMutableIndex();

        final LSMTreeIndexCursor cursor = (LSMTreeIndexCursor) mutable.range(true, null, true, null, true);
        try {
          final Field minorKeyIndexesField = LSMTreeIndexCursor.class.getDeclaredField("minorKeyIndexes");
          final Field ridStateField = LSMTreeIndexCursor.class.getDeclaredField("ridState");
          final Field mergedRIDsField = LSMTreeIndexCursor.class.getDeclaredField("mergedRIDs");
          minorKeyIndexesField.setAccessible(true);
          ridStateField.setAccessible(true);
          mergedRIDsField.setAccessible(true);

          assertThat(cursor.hasNext()).isTrue();
          final RID first = cursor.next();
          final Object minorKeyIndexesAfterFirst = minorKeyIndexesField.get(cursor);
          final Object ridStateAfterFirst = ridStateField.get(cursor);
          final Object mergedRIDsAfterFirst = mergedRIDsField.get(cursor);

          assertThat(cursor.hasNext()).isTrue();
          final RID second = cursor.next();
          assertThat(second).isNotEqualTo(first);

          assertThat(minorKeyIndexesField.get(cursor)).as("minorKeyIndexes must be the SAME instance, not reallocated per group")
              .isSameAs(minorKeyIndexesAfterFirst);
          assertThat(ridStateField.get(cursor)).as("ridState must be the SAME instance, not reallocated per group")
              .isSameAs(ridStateAfterFirst);
          assertThat(mergedRIDsField.get(cursor)).as("mergedRIDs must be the SAME instance, not reallocated per group")
              .isSameAs(mergedRIDsAfterFirst);
        } finally {
          cursor.close();
        }
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
