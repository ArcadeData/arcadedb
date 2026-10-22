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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10612: a bucket insert into a page with holes walked every slot of the page to find the first reusable hole, because
 * the summary of the record table a transaction's page keeps (#10577) only described a page without any. A page whose holes
 * were left by earlier transactions, the usual state of a bucket after deletes, took that walk for EVERY insert, so a
 * delete-then-reinsert workload paid for the page's slot count per record.
 * <p>
 * The summary now also remembers how far the search for a hole got: the slots below it are live, or holes this very
 * transaction deleted, which it may not reuse. The cost of an insert is measured by the slots it read
 * ({@link LocalBucket#getInsertSlotsReadForTesting()}), never by the clock.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BucketHoleScanTest extends TestHelper {
  private static final int RECORDS = 1_000;

  private final Map<Integer, String> model  = new HashMap<>();
  private final Map<Integer, RID>    rids   = new HashMap<>();
  private       int                  nextId = 0;

  private LocalBucket bucket() {
    return (LocalBucket) database.getSchema().getType("T").getBuckets(false).getFirst();
  }

  /** Tiny records, one bucket: the first page holds them all, so every insert lands on it. */
  private List<RID> fill(final int count) {
    final List<RID> inserted = new ArrayList<>(count);
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final MutableDocument doc = database.newDocument("T").set("id", nextId);
        doc.save();
        model.put(nextId, "");
        rids.put(nextId, doc.getIdentity());
        inserted.add(doc.getIdentity());
        nextId++;
      }
    });
    return inserted;
  }

  private void deleteSlots(final List<RID> victims) {
    database.transaction(() -> {
      for (final RID rid : victims) {
        final Document doc = database.lookupByRID(rid, true).asDocument();
        model.remove(doc.getInteger("id"));
        rids.remove(doc.getInteger("id"));
        database.deleteRecord(doc);
      }
    });
  }

  @Test
  void anInsertIntoAPageWithHolesDoesNotWalkEverySlot() {
    database.getSchema().createDocumentType("T", 1);
    final List<RID> all = fill(RECORDS);

    final List<RID> deleted = new ArrayList<>();
    for (int i = 0; i < all.size(); i += 3)
      deleted.add(all.get(i));
    deleteSlots(deleted);

    final long before = bucket().getInsertSlotsReadForTesting();
    final List<RID> reinserted = fill(300);
    final long slotsRead = bucket().getInsertSlotsReadForTesting() - before;

    // the page is the only one with room, so each reinsert takes the lowest hole a previous transaction left, as a walk would
    assertThat(reinserted).containsExactlyElementsOf(deleted.subList(0, reinserted.size()));

    // Walking every slot per insert reads about 300 x 1,000 slots. The summary reads the page twice (the transaction's first
    // insert works on the committed image, the second on its own copy) and then goes forward from where the last hole was taken
    assertThat(slotsRead).as("slots read by 300 inserts into a page of %d slots with %d holes", RECORDS, deleted.size())
        .isLessThan(10L * RECORDS);
  }

  @Test
  void aHoleDeletedInTheTransactionIsNotReusedByThatTransaction() {
    database.getSchema().createDocumentType("T", 1);
    final List<RID> all = fill(100);
    final RID committedHole = all.get(10);
    deleteSlots(List.of(committedHole));

    database.transaction(() -> {
      // slot 5 is lower than the committed hole, and is a hole this transaction made: it must be skipped, not reused
      final Document five = database.lookupByRID(all.get(5), true).asDocument();
      model.remove(five.getInteger("id"));
      rids.remove(five.getInteger("id"));
      database.deleteRecord(five);

      final MutableDocument first = database.newDocument("T").set("id", nextId);
      first.save();
      model.put(nextId, "");
      rids.put(nextId, first.getIdentity());
      nextId++;
      assertThat(first.getIdentity()).as("the hole of an earlier transaction").isEqualTo(committedHole);

      final MutableDocument second = database.newDocument("T").set("id", nextId);
      second.save();
      model.put(nextId, "");
      rids.put(nextId, second.getIdentity());
      nextId++;
      assertThat(second.getIdentity()).as("neither the hole this transaction deleted nor the one just taken")
          .isNotEqualTo(all.get(5)).isNotEqualTo(committedHole);
      // so it goes to the end of the record table
      assertThat(second.getIdentity().getPosition()).isEqualTo(all.get(99).getPosition() + 1);

      final MutableDocument third = database.newDocument("T").set("id", nextId);
      third.save();
      model.put(nextId, "");
      rids.put(nextId, third.getIdentity());
      nextId++;
      assertThat(third.getIdentity().getPosition()).isEqualTo(second.getIdentity().getPosition() + 1);
    });

    verifyModel();

    // the next transaction sees slot 5 as a committed hole
    final List<RID> next = fill(1);
    assertThat(next.getFirst()).isEqualTo(all.get(5));
    verifyModel();
  }

  @Test
  void aHoleDeletedAheadOfTheSearchIsSkippedWhileItGoesOn() {
    database.getSchema().createDocumentType("T", 1);
    final List<RID> all = fill(100);
    deleteSlots(List.of(all.get(10), all.get(30)));

    database.transaction(() -> {
      // slot 20 is a hole of this transaction between the two holes of the earlier one
      final Document twenty = database.lookupByRID(all.get(20), true).asDocument();
      model.remove(twenty.getInteger("id"));
      rids.remove(twenty.getInteger("id"));
      database.deleteRecord(twenty);

      final List<RID> taken = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        final MutableDocument doc = database.newDocument("T").set("id", nextId);
        doc.save();
        model.put(nextId, "");
        rids.put(nextId, doc.getIdentity());
        nextId++;
        taken.add(doc.getIdentity());
      }
      assertThat(taken.get(0)).as("the first hole").isEqualTo(all.get(10));
      assertThat(taken.get(1)).as("the search goes on past the hole this transaction deleted").isEqualTo(all.get(30));
      assertThat(taken.get(2).getPosition()).as("then to the end of the table").isEqualTo(all.get(99).getPosition() + 1);
    });

    verifyModel();
    assertThat(fill(1).getFirst()).as("the hole of the transaction is a committed one for the next").isEqualTo(all.get(20));
    verifyModel();
  }

  @Test
  void aFailedStatementTakenBackLeavesTheScanTrueToThePage() {
    database.getSchema().createDocumentType("T", 1);
    database.getSchema().getType("T").createProperty("uniq", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "T", "uniq");
    final List<RID> all = fill(200);
    final List<RID> deleted = new ArrayList<>();
    for (int i = 0; i < all.size(); i += 4)
      deleted.add(all.get(i));
    deleteSlots(deleted);

    database.begin();
    final MutableDocument first = database.newDocument("T").set("id", nextId).set("uniq", 1);
    first.save();
    model.put(nextId, "");
    rids.put(nextId, first.getIdentity());
    nextId++;
    assertThat(first.getIdentity()).isEqualTo(deleted.get(0));

    // writes its first row, then refuses the second as a duplicate: the statement is taken back to where it started
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO T (id, uniq) VALUES (9001, 2), (9002, 2)").close())
        .isInstanceOf(DuplicatedKeyException.class);

    final MutableDocument second = database.newDocument("T").set("id", nextId).set("uniq", 3);
    second.save();
    model.put(nextId, "");
    rids.put(nextId, second.getIdentity());
    nextId++;
    assertThat(second.getIdentity()).as("the next hole, as if the failed statement never ran").isEqualTo(deleted.get(1));
    database.commit();

    verifyModel();
    checkDatabaseIntegrity();
  }

  @Test
  void randomDeletesAndReinsertsOverSeveralPagesMatchAModelAcrossReopen() {
    database.getSchema().createDocumentType("T", 1);
    final Random rnd = new Random(10612);

    fill(6_000);
    for (int round = 0; round < 8; round++) {
      final List<RID> candidates = new ArrayList<>(rids.values());
      final List<RID> victims = new ArrayList<>();
      for (final RID rid : candidates)
        if (rnd.nextInt(100) < 30)
          victims.add(rid);
      deleteSlots(victims);
      verifyModel();

      // reinserts of several sizes, some in the transaction that also deletes
      fill(500 + rnd.nextInt(2_000));
      database.transaction(() -> {
        for (int i = 0; i < 200; i++) {
          final List<Integer> keys = new ArrayList<>(model.keySet());
          final int id = keys.get(rnd.nextInt(keys.size()));
          database.deleteRecord(database.lookupByRID(rids.get(id), true));
          model.remove(id);
          rids.remove(id);
          final MutableDocument doc = database.newDocument("T").set("id", nextId);
          doc.save();
          model.put(nextId, "");
          rids.put(nextId, doc.getIdentity());
          nextId++;
        }
      });
      verifyModel();
    }

    reopenDatabase();
    verifyModel();
    checkDatabaseIntegrity();
  }

  private void verifyModel() {
    assertThat(database.countType("T", true)).isEqualTo(model.size());
    final Map<Integer, String> seen = new HashMap<>();
    database.iterateType("T", true).forEachRemaining(r -> assertThat(seen.put(r.asDocument().getInteger("id"), "")).isNull());
    assertThat(seen).isEqualTo(model);
    for (final Map.Entry<Integer, RID> e : rids.entrySet())
      assertThat(database.lookupByRID(e.getValue(), true).asDocument().getInteger("id")).isEqualTo(e.getKey());
  }
}
