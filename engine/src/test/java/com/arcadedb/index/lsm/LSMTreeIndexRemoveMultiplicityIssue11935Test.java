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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #11935: removing a (key, RID) from a NOTUNIQUE or FULL_TEXT LSM index deserialized every
 * value of the key held by the last page, newest first, only to decide whether the removal marker about to be appended would
 * repeat one already there. Removing all n records of a shared key read O(n^2) values: a bulk UPDATE or DELETE of 40,000
 * records sharing a value took 20 s. The removal now reads at most the newest entry of the key. The cost is measured by the
 * single values the removals read ({@link LSMTreeIndexMutable#getRemoveValuesReadForTesting()}), never by the clock, at n and
 * 4n records: linear work grows about 4x, the old scan about 16x. The perturbation this catches is any return to reading the
 * key's older entries (the values of the multi-value put, or the markers of earlier removals) for each removal.
 *
 * @author Shaun Patterson (shaunpatterson@gmail.com)
 */
class LSMTreeIndexRemoveMultiplicityIssue11935Test extends TestHelper {
  private static final int N = 1_000;

  @BeforeEach
  @Override
  public void beforeTest() {
    // keep compaction manual: the counter belongs to the mutable index the test captured
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  private LSMTreeIndex bucketIndex(final String indexName) {
    final Index index = database.getSchema().getIndexByName(indexName);
    final Index[] onBuckets = ((TypeIndex) index).getIndexesOnBuckets();
    assertThat(onBuckets).hasSize(1);
    return (LSMTreeIndex) onBuckets[0];
  }

  /** Values the removals read while {@code statement} removed every one of {@code records} records sharing one key from the index. */
  private long valuesReadRemovingAllOf(final String type, final int records, final String statement) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + type + ".c STRING");
    database.command("sql", "CREATE INDEX ON " + type + " (c) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        database.newDocument(type).set("c", "same").save();
    });

    final LSMTreeIndexMutable mutable = bucketIndex(type + "[c]").getMutableIndex();
    final long before = mutable.getRemoveValuesReadForTesting();
    database.transaction(() -> database.command("sql", statement.replace("$type", type)));
    return mutable.getRemoveValuesReadForTesting() - before;
  }

  @Test
  void deletingEveryRecordOfASharedKeyReadsLinearlyManyValues() {
    final long small = valuesReadRemovingAllOf("D1", N, "DELETE FROM $type");
    final long large = valuesReadRemovingAllOf("D4", 4 * N, "DELETE FROM $type");

    // ~4x for a removal that costs the same whatever the key's multiplicity, ~16x for one that reads the key's values
    assertThat(large).as("growth from %d (%d values) to %d records", N, small, 4 * N).isLessThan(8L * Math.max(small, 1));
    assertThat(large).as("values read removing %d records of one key", 4 * N).isLessThanOrEqualTo(4L * N);
    assertThat(database.countType("D4", true)).isZero();
    assertThat(lookup("D4[c]", "same")).isEmpty();
  }

  @Test
  void updatingEveryRecordOfASharedKeyReadsLinearlyManyValues() {
    final long small = valuesReadRemovingAllOf("U1", N, "UPDATE $type SET c = 'other'");
    final long large = valuesReadRemovingAllOf("U4", 4 * N, "UPDATE $type SET c = 'other'");

    assertThat(large).as("growth from %d (%d values) to %d records", N, small, 4 * N).isLessThan(8L * Math.max(small, 1));
    assertThat(large).as("values read removing %d records of one key", 4 * N).isLessThanOrEqualTo(4L * N);
    assertThat(lookup("U4[c]", "same")).isEmpty();
    assertThat(lookup("U4[c]", "other")).hasSize(4 * N);
  }

  @Test
  void fullTextRemovalsReadLinearlyManyValues() {
    final long[] read = new long[2];
    final int[] sizes = { N, 4 * N };
    for (int s = 0; s < 2; s++) {
      final String type = "F" + s;
      final int records = sizes[s];
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 1");
      database.command("sql", "CREATE PROPERTY " + type + ".s STRING");
      database.command("sql", "CREATE INDEX ON " + type + " (s) FULL_TEXT");
      database.transaction(() -> {
        for (int i = 0; i < records; i++)
          database.newDocument(type).set("s", "shared text").save();
      });
      final Index index = database.getSchema().getIndexByName(type + "[s]");
      // the full-text index answers with the file id of the LSM-Tree it writes through
      final LSMTreeIndexMutable mutable = (LSMTreeIndexMutable) database.getSchema().getEmbedded()
          .getFileById(((TypeIndex) index).getIndexesOnBuckets()[0].getFileId());
      final long before = mutable.getRemoveValuesReadForTesting();
      database.transaction(() -> database.command("sql", "UPDATE " + type + " SET s = 'changed words'"));
      read[s] = mutable.getRemoveValuesReadForTesting() - before;
    }

    // two shared tokens per record
    assertThat(read[1]).as("growth from %d (%d values) to %d records", N, read[0], 4 * N).isLessThan(8L * Math.max(read[0], 1));
    assertThat(read[1]).as("values read removing the tokens of %d records", 4 * N).isLessThanOrEqualTo(8L * N);
    try (final var rs = database.query("sql", "SELECT count(*) AS c FROM F1 WHERE SEARCH_INDEX('F1[s]', 'shared') = true")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
    try (final var rs = database.query("sql", "SELECT count(*) AS c FROM F1 WHERE SEARCH_INDEX('F1[s]', 'changed') = true")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(4L * N);
    }
  }

  @Test
  void aRemovalRepeatedAcrossTransactionsReinsertAndCompactionKeepTheirMeaning() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE R BUCKETS 1");
    database.command("sql", "CREATE PROPERTY R.c STRING");
    // Small pages, so the records written after the history below fill the page holding it and the compaction has an immutable
    // page to compact: a compaction of an index of one page does nothing
    database.getSchema().buildTypeIndex("R", new String[] { "c" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
        .withPageSize(2048).create();
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        rids.add(database.newDocument("R").set("c", "k").save().getIdentity());
    });
    final Index index = database.getSchema().getIndexByName("R[c]");
    final LSMTreeIndex onBucket = bucketIndex("R[c]");
    final RID a = rids.get(0);
    final RID b = rids.get(1);

    // removed twice in a row: the second removal finds its own marker as the key's newest entry and writes nothing
    database.transaction(() -> index.remove(new Object[] { "k" }, a));
    database.transaction(() -> index.remove(new Object[] { "k" }, a));
    assertThat(lookup("R[c]", "k")).containsExactlyInAnyOrder(b, rids.get(2));

    // removed again after another RID's marker: a second marker for `a`, which changes nothing a reader sees
    database.transaction(() -> index.remove(new Object[] { "k" }, b));
    database.transaction(() -> index.remove(new Object[] { "k" }, a));
    assertThat(lookup("R[c]", "k")).containsExactly(rids.get(2));

    // re-inserted after its markers: alive again, the newest entry wins
    database.transaction(() -> onBucket.put(new Object[] { "k" }, new RID[] { a }));
    assertThat(lookup("R[c]", "k")).containsExactlyInAnyOrder(a, rids.get(2));

    // removed and re-inserted in one transaction: the commit replays the removal first, so the RID stays
    database.transaction(() -> {
      index.remove(new Object[] { "k" }, a);
      onBucket.put(new Object[] { "k" }, new RID[] { a });
    });
    assertThat(lookup("R[c]", "k")).containsExactlyInAnyOrder(a, rids.get(2));
    assertThat(index.countEntries()).isEqualTo(2);

    // the compaction resolves the same history: more records with other keys fill the page holding it, which the compaction then
    // merges into the compacted index
    final int others = 400;
    database.transaction(() -> {
      for (int i = 0; i < others; i++)
        database.newDocument("R").set("c", "other-%04d".formatted(i)).save();
    });
    final int pagesBefore = onBucket.getMutableIndex().getTotalPages();
    assertThat(pagesBefore).as("pages of the mutable index before the compaction").isGreaterThanOrEqualTo(2);
    assertThat(onBucket.getMutableIndex().getSubIndex()).as("compacted index before the compaction").isNull();

    onBucket.scheduleCompaction();
    assertThat(onBucket.compact()).as("the compaction ran").isTrue();
    assertThat(onBucket.getMutableIndex().getSubIndex()).as("compacted index after the compaction").isNotNull();
    assertThat(onBucket.getMutableIndex().getTotalPages()).as("pages left in the mutable index").isLessThan(pagesBefore);
    assertThat(lookup("R[c]", "k")).containsExactlyInAnyOrder(a, rids.get(2));
    assertThat(index.countEntries()).isEqualTo(2 + others);

    // and a key-wide removal followed by a per-RID one stays a removal of everything
    database.transaction(() -> index.remove(new Object[] { "k" }));
    database.transaction(() -> index.remove(new Object[] { "k" }, a));
    assertThat(lookup("R[c]", "k")).isEmpty();

    // the index was edited behind the records' back: drop them, so the database check after the test has nothing to report
    database.transaction(() -> database.command("sql", "DELETE FROM R"));
  }

  private Set<RID> lookup(final String indexName, final String key) {
    final Set<RID> found = new HashSet<>();
    final IndexCursor cursor = database.getSchema().getIndexByName(indexName).get(new Object[] { key });
    while (cursor.hasNext())
      found.add(cursor.next().getIdentity());
    return found;
  }
}
