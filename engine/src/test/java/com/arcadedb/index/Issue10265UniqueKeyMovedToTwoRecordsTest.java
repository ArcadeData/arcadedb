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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10265: on a UNIQUE index a transaction that took a key away from its committed holder and then gave it to
 * TWO records committed both. The first new claim was a {@code REPLACE} (a same-key REMOVE + ADD merged into one
 * entry), and the in-transaction duplicate check only treated a pending {@code ADD} as a claim, so the second ADD
 * displaced the first in the per-key map: two records held the key and the index kept one of them.
 * <p>
 * The comment cases H and I: a key claimed and withdrawn by the same uncommitted record left a pending {@code REMOVE}
 * of that record, which a read of a unique index treats as removing the whole key, hiding the committed holder from
 * the transaction's own lookups until commit.
 */
class Issue10265UniqueKeyMovedToTwoRecordsTest extends TestHelper {
  private static final AtomicInteger TYPES = new AtomicInteger();

  private String type;
  private RID    x;
  private RID    y;

  /** A fresh single-bucket type holding X (k = 1) and, when asked, Y (k = 2). */
  private void fresh(final String kind, final int buckets, final boolean withY) {
    type = "T" + TYPES.incrementAndGet();
    database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS " + buckets);
    database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
    database.command("sql", "CREATE INDEX ON " + type + " (k) " + kind);
    database.transaction(() -> {
      x = insert(1, 0);
      if (withY)
        y = insert(2, 0);
    });
  }

  private String bucket(final int i) {
    return database.getSchema().getType(type).getBuckets(false).get(i).getName();
  }

  private RID insert(final Object k, final int bucket) {
    final MutableDocument d = database.newDocument(type).set("k", k);
    d.save(bucket(bucket));
    return d.getIdentity();
  }

  private RID insert(final Object k) {
    return insert(k, 0);
  }

  private void set(final RID rid, final Object k) {
    rid.asDocument().modify().set("k", k).save();
  }

  private int holders(final int k) {
    int holders = 0;
    for (final Iterator<Record> it = database.iterateType(type, true); it.hasNext(); ) {
      final Document d = it.next().asDocument();
      if (Objects.equals(d.get("k"), k))
        ++holders;
    }
    return holders;
  }

  private int indexGet(final int k) {
    int found = 0;
    final IndexCursor c = database.getSchema().getIndexByName(type + "[k]").get(new Object[] { k });
    while (c.hasNext()) {
      c.next();
      ++found;
    }
    return found;
  }

  private long sqlCount(final int k) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE k = " + k)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  /** The records holding k, what the index answers for it and the SQL count through the index all agree on n. */
  private void assertKeyHeldBy(final int k, final int n) {
    assertThat(holders(k)).as("records holding k=" + k).isEqualTo(n);
    assertThat(indexGet(k)).as("index get(" + k + ")").isEqualTo(n);
    assertThat(sqlCount(k)).as("SELECT count(*) WHERE k = " + k).isEqualTo(n);
  }

  private void assertRefused(final Runnable tx) {
    assertThatThrownBy(() -> database.transaction(tx::run)).isInstanceOf(DuplicatedKeyException.class);
    // refused: the committed holder X still owns the key, and the index still finds it
    assertKeyHeldBy(1, 1);
    assertThat(indexGetRid(1)).isEqualTo(x);
  }

  private RID indexGetRid(final int k) {
    return database.getSchema().getIndexByName(type + "[k]").get(new Object[] { k }).next().getIdentity();
  }

  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void caseCReplacementStillCommits(final String kind) {
    fresh(kind, 1, false);
    database.transaction(() -> {
      x.asDocument().delete();
      insert(1);
    });
    assertKeyHeldBy(1, 1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void caseDDeleteHolderThenInsertTwiceIsRefused(final String kind) {
    fresh(kind, 1, false);
    assertRefused(() -> {
      x.asDocument().delete();
      insert(1);
      insert(1);
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void caseEMoveHolderOffThenInsertTwiceIsRefused(final String kind) {
    fresh(kind, 1, false);
    assertRefused(() -> {
      set(x, 2);
      insert(1);
      insert(1);
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void caseFRewriteOfTheNewHolderThenInsertIsRefused(final String kind) {
    fresh(kind, 1, false);
    assertRefused(() -> {
      x.asDocument().delete();
      final RID n = insert(1);
      set(n, 1);
      insert(1);
    });
  }

  /**
   * G: Z takes k = 1 while X still holds it (legal until commit, X then moves off), then Y moves onto k = 1 while Z
   * holds it - two uncommitted records claiming one key, the same shape as inserting k = 1 twice (case A), which is
   * refused at once. It used to be accepted and to drop Z's claim, so the commit left Z holding 1 and the index empty.
   */
  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void caseGSecondUncommittedClaimIsRefused(final String kind) {
    fresh(kind, 1, true);
    assertRefused(() -> {
      insert(1);
      set(x, null);
      set(y, 1);
      set(y, 3);
    });
    assertKeyHeldBy(2, 1);
  }

  /** G without the transient duplicate: the moves alone must keep the index on the record that ends with the key. */
  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void caseGWithoutTheTransientDuplicateCommitsConsistently(final String kind) {
    fresh(kind, 1, true);
    database.transaction(() -> {
      set(x, null);
      insert(1);
      set(y, 3);
    });
    assertKeyHeldBy(1, 1);
    assertKeyHeldBy(3, 1);
    assertKeyHeldBy(2, 0);
  }

  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void sqlScriptDeleteThenInsertTwiceIsRefused(final String kind) {
    fresh(kind, 1, false);
    assertThatThrownBy(() -> database.command("sqlscript",
        "BEGIN; DELETE FROM " + type + " WHERE k = 1; INSERT INTO " + type + " SET k = 1; INSERT INTO " + type + " SET k = 1; COMMIT;")
        .close()).isInstanceOf(DuplicatedKeyException.class);
    if (database.isTransactionActive())
      database.rollback();
    assertKeyHeldBy(1, 1);
  }

  /** The two new claims land in DIFFERENT buckets, so the duplicate sits in another sub-index's lane. */
  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void crossBucketDeleteThenInsertTwiceIsRefused(final String kind) {
    fresh(kind, 4, false);
    assertRefused(() -> {
      x.asDocument().delete();
      insert(1, 1);
      insert(1, 2);
    });
  }

  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void crossBucketMoveOffThenInsertTwiceIsRefused(final String kind) {
    fresh(kind, 4, false);
    assertRefused(() -> {
      set(x, 2);
      insert(1, 0);
      insert(1, 3);
    });
  }

  /** Comment case H: insert Y k = 1 (refused at commit), move it to 2: the committed X must stay visible inside the tx. */
  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void caseHWithdrawnClaimKeepsTheCommittedHolderVisible(final String kind) {
    fresh(kind, 1, false);
    database.transaction(() -> {
      final RID n = insert(1);
      set(n, 2);
      assertKeyHeldBy(1, 1);
      assertThat(indexGetRid(1)).isEqualTo(x);
    });
    assertKeyHeldBy(1, 1);
    assertKeyHeldBy(2, 1);
  }

  /** Comment case I: insert Y k = 1, delete it: the committed X must stay visible inside the tx. */
  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void caseIInsertedThenDeletedKeepsTheCommittedHolderVisible(final String kind) {
    fresh(kind, 1, false);
    database.transaction(() -> {
      insert(1).asDocument().delete();
      assertKeyHeldBy(1, 1);
      assertThat(indexGetRid(1)).isEqualTo(x);
    });
    assertKeyHeldBy(1, 1);
  }

  /** H then a real replacement: the withdrawn claim must not swallow the removal of X that follows. */
  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void withdrawnClaimThenReplacementCommits(final String kind) {
    fresh(kind, 1, false);
    database.transaction(() -> {
      final RID n = insert(1);
      n.asDocument().delete();
      x.asDocument().delete();
      insert(1);
    });
    assertKeyHeldBy(1, 1);
  }

  /**
   * The reporter's single-thread workload: random transactions of inserts, key updates and deletes over 16 keys, the
   * index checked against a scan after every transaction, committed or refused. It broke the index in 33 of 40 seeds.
   */
  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void randomSingleThreadWorkloadKeepsTheIndexInStepWithTheRecords(final String kind) {
    fresh(kind, 2, false);
    for (long seed = 0; seed < 40; seed++) {
      final Random random = new Random(seed);
      for (int t = 0; t < 30; t++) {
        final List<RID> committed = new ArrayList<>();
        for (final Iterator<Record> it = database.iterateType(type, true); it.hasNext(); )
          committed.add(it.next().getIdentity());
        // the same operations on every attempt, should the transaction be retried
        final long txSeed = random.nextLong();
        try {
          database.transaction(() -> {
            final Random ops0 = new Random(txSeed);
            final List<RID> live = new ArrayList<>(committed);
            final int ops = 1 + ops0.nextInt(5);
            for (int o = 0; o < ops; o++) {
              final int op = ops0.nextInt(3);
              if (op == 0 || live.isEmpty())
                live.add(insert(ops0.nextInt(16), ops0.nextInt(2)));
              else if (op == 1)
                set(live.get(ops0.nextInt(live.size())), ops0.nextInt(16));
              else
                live.remove(ops0.nextInt(live.size())).asDocument().delete();
            }
          });
        } catch (final DuplicatedKeyException refused) {
          // refused as a whole: the check below still holds
        }

        final Map<Object, Integer> holders = new HashMap<>();
        for (final Iterator<Record> it = database.iterateType(type, true); it.hasNext(); )
          holders.merge(it.next().asDocument().get("k"), 1, Integer::sum);
        for (int k = 0; k < 16; k++) {
          final int held = holders.getOrDefault(k, 0);
          assertThat(held).as("seed %d tx %d: records holding k=%d", seed, t, k).isLessThanOrEqualTo(1);
          assertThat(indexGet(k)).as("seed %d tx %d: index get(%d)", seed, t, k).isEqualTo(held);
        }
      }
    }
  }

  /**
   * A withdrawn claim in one bucket and the committed holder's removal in another both read as "the key was deleted in
   * this transaction": the replacement in a third bucket must still commit, whichever removal is looked at first.
   */
  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void crossBucketWithdrawnClaimThenReplacementCommits(final String kind) {
    fresh(kind, 4, false);
    database.transaction(() -> {
      x.asDocument().delete();
      insert(1, 1).asDocument().delete();
      insert(1, 2);
    });
    assertKeyHeldBy(1, 1);

    fresh(kind, 4, false);
    database.transaction(() -> {
      insert(1, 1).asDocument().delete();
      x.asDocument().delete();
      insert(1, 2);
    });
    assertKeyHeldBy(1, 1);
  }

  /**
   * A removal naming a RID that does not hold the key is a no-op at commit, on HASH as on LSM: before #10265 the HASH bucket
   * ignored the RID of a unique key and dropped the holder's entry with it.
   */
  @ParameterizedTest
  @ValueSource(strings = { "UNIQUE", "UNIQUE_HASH" })
  void removalNamingAnotherRidLeavesTheHolderIndexed(final String kind) {
    fresh(kind, 1, true);
    database.transaction(() -> database.getSchema().getIndexByName(type + "[k]").remove(new Object[] { 1 }, y));
    assertKeyHeldBy(1, 1);
    assertThat(indexGetRid(1)).isEqualTo(x);
  }
}
