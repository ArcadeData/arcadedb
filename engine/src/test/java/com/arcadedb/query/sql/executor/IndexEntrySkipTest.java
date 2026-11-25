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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.IndexEntrySkip.Verdict;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #11954: what {@link IndexEntrySkip} tells a SKIP about the row of an index entry, from the record's slot and not from loading
 * it, for the entries of an index that spans several buckets and so comes interleaved.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class IndexEntrySkipTest extends TestHelper {
  private static final int BUCKETS = 4;

  private final List<RID> wide = new ArrayList<>();
  private final List<RID> item = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.getSchema().buildVertexType().withName("Wide").withTotalBuckets(BUCKETS).create();
    database.getSchema().buildVertexType().withName("Item").withTotalBuckets(BUCKETS).withSuperType("Wide").create();
    database.transaction(() -> {
      for (int i = 0; i < 40; i++) {
        final MutableVertex vertex = database.newVertex(i % 2 == 0 ? "Wide" : "Item").save();
        (i % 2 == 0 ? wide : item).add(vertex.getIdentity());
      }
    });
  }

  @Test
  void aRecordThatIsThereIsARowWhateverTheBucketItIsIn() {
    final IndexEntrySkip judge = new IndexEntrySkip((DatabaseInternal) database, null);
    // the records are in every bucket of both types: the answers come interleaved, as the entries of an index do
    assertThat(bucketsOf(wide)).hasSize(BUCKETS);
    assertThat(bucketsOf(item)).hasSize(BUCKETS);
    for (int i = 0; i < wide.size(); i++) {
      assertThat(judge.verdict(wide.get(i))).isEqualTo(Verdict.ROW);
      assertThat(judge.verdict(item.get(i))).isEqualTo(Verdict.ROW);
    }
  }

  @Test
  void aRecordOfAnotherTypeThanTheRowsIsNoRow() {
    final IndexEntrySkip judge = new IndexEntrySkip((DatabaseInternal) database, "Item");
    for (int i = 0; i < wide.size(); i++) {
      assertThat(judge.verdict(wide.get(i))).isEqualTo(Verdict.NOT_A_ROW);
      assertThat(judge.verdict(item.get(i))).isEqualTo(Verdict.ROW);
    }
  }

  @Test
  void aRecordThatIsGoneHasToBeLoaded() {
    final RID gone = wide.getFirst();
    database.transaction(() -> gone.asVertex().delete());

    final IndexEntrySkip judge = new IndexEntrySkip((DatabaseInternal) database, null);
    assertThat(judge.verdict(gone)).isEqualTo(Verdict.LOAD);
    assertThat(judge.verdict(wide.get(1))).isEqualTo(Verdict.ROW);
  }

  @Test
  void aRecordDeletedByTheTransactionHasToBeLoaded() {
    database.begin();
    try {
      final RID gone = item.getFirst();
      gone.asVertex().delete();

      final IndexEntrySkip judge = new IndexEntrySkip((DatabaseInternal) database, null);
      assertThat(judge.verdict(gone)).isEqualTo(Verdict.LOAD);
      assertThat(judge.verdict(item.get(1))).isEqualTo(Verdict.ROW);
    } finally {
      database.rollback();
    }
  }

  @Test
  void aTypeThatFiresReadEventsHasItsRecordsLoaded() {
    final AfterRecordReadListener listener = record -> record;
    database.getSchema().getType("Item").getEvents().registerListener(listener);
    try {
      final IndexEntrySkip judge = new IndexEntrySkip((DatabaseInternal) database, null);
      for (int i = 0; i < wide.size(); i++) {
        // the listener of Item is not the one of Wide: only the records of Item are loaded
        assertThat(judge.verdict(wide.get(i))).isEqualTo(Verdict.ROW);
        assertThat(judge.verdict(item.get(i))).isEqualTo(Verdict.LOAD);
      }
    } finally {
      database.getSchema().getType("Item").getEvents().unregisterListener(listener);
    }
  }

  @Test
  void aBucketTheSchemaDoesNotKnowHasItsRecordsLoaded() {
    final IndexEntrySkip judge = new IndexEntrySkip((DatabaseInternal) database, null);
    // no bucket has this id, however far past the buckets of the database it is
    assertThat(judge.verdict(new RID(-1, 0))).isEqualTo(Verdict.LOAD);
    assertThat(judge.verdict(new RID(Integer.MAX_VALUE, 0))).isEqualTo(Verdict.LOAD);
    assertThat(judge.verdict(new RID(9_000, 0))).isEqualTo(Verdict.LOAD);
    assertThat(judge.verdict(wide.getFirst())).isEqualTo(Verdict.ROW);
  }

  private static List<Integer> bucketsOf(final List<RID> rids) {
    return rids.stream().map(RID::getBucketId).distinct().toList();
  }
}
