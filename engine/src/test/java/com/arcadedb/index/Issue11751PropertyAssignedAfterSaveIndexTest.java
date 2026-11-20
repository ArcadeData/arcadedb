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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11751: a property assigned after the last {@code save()} of a record and before the commit is written with the record (the
 * commit serializes the very instance it queued) but was not indexed, because the index follows the {@code save()} that queued its
 * changes and the commit flush of the deferred update does not index again (#8983). The record then disagreed with its index:
 * {@code WHERE k = 1} found a row whose k is 2, {@code WHERE k = 2} found nothing, a UNIQUE index never saw the key the record ended
 * with, and {@code CHECK DATABASE} (#11734) reported a stale and a missing entry.
 * <p>
 * Every assertion on an index reads the index itself: a query could be answered by a scan and hide a stale entry.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11751PropertyAssignedAfterSaveIndexTest extends TestHelper {

  private static final String[] ALL_KINDS    = { "NOTUNIQUE", "NOTUNIQUE_HASH", "UNIQUE", "UNIQUE_HASH" };
  private static final String[] UNIQUE_KINDS = { "UNIQUE", "UNIQUE_HASH" };

  private String create(final String prefix, final String kind) {
    final String type = prefix + "_" + kind;
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
    database.command("sql", "CREATE INDEX ON " + type + " (k) " + kind);
    return type;
  }

  private List<String> lookup(final String type, final Object... key) {
    final List<String> rids = new ArrayList<>();
    final IndexCursor cursor = database.getSchema().getType(type).getPolymorphicIndexByProperties("k").get(key);
    while (cursor.hasNext())
      rids.add(cursor.next().getIdentity().toString());
    return rids;
  }

  private long countEntries(final String type) {
    return database.getSchema().getType(type).getPolymorphicIndexByProperties("k").countEntries();
  }

  private RID insert(final String type, final int k) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(type).set("k", k).save().getIdentity());
    return rid[0];
  }

  private MutableDocument load(final RID rid) {
    return database.lookupByRID(rid, true).asDocument().modify();
  }

  private void assertCheckDatabaseFindsNoIndexDisagreement() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result check = rs.next();
      assertThat(check.<Long>getProperty("staleIndexEntries")).isZero();
      assertThat(check.<Long>getProperty("missingIndexEntries")).isZero();
    }
  }

  @Test
  void keyAssignedAfterTheLastSaveIsIndexed() {
    for (final String kind : ALL_KINDS) {
      final String t = create("A", kind);
      final RID rid = insert(t, 0);

      database.transaction(() -> {
        final MutableDocument d = load(rid);
        d.set("k", 1).save();
        d.set("k", 2);
      });

      assertThat(lookup(t, 2)).as(kind).containsExactly(rid.toString());
      assertThat(lookup(t, 1)).as(kind).isEmpty();
      assertThat(lookup(t, 0)).as(kind).isEmpty();
      assertThat(countEntries(t)).as(kind).isEqualTo(1);
      assertThat(load(rid).getInteger("k")).as(kind).isEqualTo(2);
    }
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void keyAssignedAfterTheOnlySaveOfTheTransactionIsIndexed() {
    // the saved value is the committed one here: the diff source is the committed buffer, not a snapshot of an earlier index change
    for (final String kind : ALL_KINDS) {
      final String t = create("O", kind);
      final RID rid = insert(t, 0);

      database.transaction(() -> {
        final MutableDocument d = load(rid);
        d.set("k", 0).save();
        d.set("k", 7);
      });

      assertThat(lookup(t, 7)).as(kind).containsExactly(rid.toString());
      assertThat(lookup(t, 0)).as(kind).isEmpty();
      assertThat(countEntries(t)).as(kind).isEqualTo(1);
    }
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void lastOfSeveralAssignmentsAfterTheSaveIsTheIndexedOne() {
    for (final String kind : ALL_KINDS) {
      final String t = create("M", kind);
      final RID rid = insert(t, 0);

      database.transaction(() -> {
        final MutableDocument d = load(rid);
        d.set("k", 1).save();
        d.set("k", 2);
        d.set("k", 3);
      });

      assertThat(lookup(t, 3)).as(kind).containsExactly(rid.toString());
      for (final int gone : new int[] { 0, 1, 2 })
        assertThat(lookup(t, gone)).as(kind + " k=" + gone).isEmpty();
      assertThat(countEntries(t)).as(kind).isEqualTo(1);
    }
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void assignmentBackToTheSavedValueLeavesTheIndexAlone() {
    for (final String kind : ALL_KINDS) {
      final String t = create("B", kind);
      final RID rid = insert(t, 0);

      database.transaction(() -> {
        final MutableDocument d = load(rid);
        d.set("k", 1).save();
        d.set("k", 2);
        d.set("k", 1);
      });

      assertThat(lookup(t, 1)).as(kind).containsExactly(rid.toString());
      assertThat(lookup(t, 2)).as(kind).isEmpty();
      assertThat(lookup(t, 0)).as(kind).isEmpty();
      assertThat(countEntries(t)).as(kind).isEqualTo(1);
    }
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void recordCreatedAndSavedAgainInTheTransactionIsIndexedWithItsLastKey() {
    for (final String kind : ALL_KINDS) {
      final String t = create("N", kind);
      final RID[] rid = new RID[1];

      database.transaction(() -> {
        final MutableDocument d = database.newDocument(t).set("k", 0).save();
        rid[0] = d.getIdentity();
        d.set("k", 1).save();
        d.set("k", 2);
      });

      assertThat(lookup(t, 2)).as(kind).containsExactly(rid[0].toString());
      assertThat(lookup(t, 1)).as(kind).isEmpty();
      assertThat(lookup(t, 0)).as(kind).isEmpty();
      assertThat(countEntries(t)).as(kind).isEqualTo(1);
    }
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void uniqueIndexRefusesAKeyAssignedAfterTheSaveThatAnotherRecordHolds() {
    for (final String kind : UNIQUE_KINDS) {
      final String t = create("D", kind);
      final RID first = insert(t, 1);
      final RID second = insert(t, 2);

      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableDocument d = load(first);
        d.set("k", 3).save();
        d.set("k", 2);
      })).as(kind).isInstanceOf(DuplicatedKeyException.class);

      // the transaction was rolled back: both records and both entries are as they were
      assertThat(load(first).getInteger("k")).as(kind).isEqualTo(1);
      assertThat(load(second).getInteger("k")).as(kind).isEqualTo(2);
      assertThat(lookup(t, 1)).as(kind).containsExactly(first.toString());
      assertThat(lookup(t, 2)).as(kind).containsExactly(second.toString());
      assertThat(lookup(t, 3)).as(kind).isEmpty();
      assertThat(countEntries(t)).as(kind).isEqualTo(2);
    }
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void uniqueIndexAcceptsAKeyAssignedAfterTheSaveThatAnotherRecordLetGoInTheSameTransaction() {
    for (final String kind : UNIQUE_KINDS) {
      final String t = create("F", kind);
      final RID first = insert(t, 1);
      final RID second = insert(t, 2);

      database.transaction(() -> {
        final MutableDocument a = load(first);
        a.set("k", 3).save();
        a.set("k", 2);
        load(second).set("k", 5).save();
      });

      assertThat(lookup(t, 2)).as(kind).containsExactly(first.toString());
      assertThat(lookup(t, 5)).as(kind).containsExactly(second.toString());
      assertThat(lookup(t, 1)).as(kind).isEmpty();
      assertThat(lookup(t, 3)).as(kind).isEmpty();
      assertThat(countEntries(t)).as(kind).isEqualTo(2);
    }
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void everyRecordOfABatchIsIndexedWithItsLastKey() {
    final String t = create("W", "UNIQUE");
    final int total = 200;
    final RID[] rids = new RID[total];
    database.transaction(() -> {
      for (int i = 0; i < total; i++)
        rids[i] = database.newDocument(t).set("k", i).save().getIdentity();
    });

    // even records are moved after their save, odd ones are saved only: both kinds share the transaction
    database.transaction(() -> {
      for (int i = 0; i < total; i++) {
        final MutableDocument d = load(rids[i]);
        d.set("k", i + 1000).save();
        if (i % 2 == 0)
          d.set("k", i + 5000);
      }
    });

    for (int i = 0; i < total; i++) {
      final int expected = i % 2 == 0 ? i + 5000 : i + 1000;
      assertThat(lookup(t, expected)).as("record " + i).containsExactly(rids[i].toString());
      assertThat(lookup(t, i)).as("old key of record " + i).isEmpty();
      if (i % 2 == 0)
        assertThat(lookup(t, i + 1000)).as("saved key of record " + i).isEmpty();
    }
    assertThat(countEntries(t)).isEqualTo(total);
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void compositeKeyFollowsAPropertyAssignedAfterTheSave() {
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.first STRING");
    database.command("sql", "CREATE PROPERTY Person.last STRING");
    database.command("sql", "CREATE INDEX ON Person (first, last) NOTUNIQUE");

    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Person").set("first", "John").set("last", "Doe").save().getIdentity());

    database.transaction(() -> {
      final MutableDocument d = load(rid[0]);
      d.set("first", "Jane").save();
      d.set("last", "Smith");
    });

    final TypeIndex index = database.getSchema().getType("Person").getPolymorphicIndexByProperties("first", "last");
    assertThat(index.get(new Object[] { "Jane", "Smith" }).hasNext()).isTrue();
    assertThat(index.get(new Object[] { "Jane", "Doe" }).hasNext()).isFalse();
    assertThat(index.get(new Object[] { "John", "Doe" }).hasNext()).isFalse();
    assertThat(index.countEntries()).isEqualTo(1);
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void listByItemIndexFollowsAListAssignedAfterTheSave() {
    database.command("sql", "CREATE DOCUMENT TYPE Tagged");
    database.command("sql", "CREATE PROPERTY Tagged.tags LIST");
    database.command("sql", "CREATE INDEX ON Tagged (tags BY ITEM) NOTUNIQUE");

    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Tagged").set("tags", List.of("a", "b")).save().getIdentity());

    database.transaction(() -> {
      final MutableDocument d = load(rid[0]);
      d.set("tags", List.of("b", "c")).save();
      d.set("tags", List.of("c", "d"));
    });

    final TypeIndex index = database.getSchema().getType("Tagged").getAllIndexes(true).iterator().next();
    for (final String gone : new String[] { "a", "b" })
      assertThat(index.get(new Object[] { gone }).hasNext()).as("item " + gone).isFalse();
    for (final String kept : new String[] { "c", "d" }) {
      final IndexCursor cursor = index.get(new Object[] { kept });
      assertThat(cursor.hasNext()).as("item " + kept).isTrue();
      assertThat(cursor.next().getIdentity()).as("item " + kept).isEqualTo(rid[0]);
    }
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void aPropertyNoIndexCoversIsStillWrittenWithTheRecord() {
    final String t = create("P", "NOTUNIQUE");
    final RID rid = insert(t, 0);

    database.transaction(() -> {
      final MutableDocument d = load(rid);
      d.set("k", 1).save();
      d.set("note", "after the save");
    });

    assertThat(load(rid).getString("note")).isEqualTo("after the save");
    assertThat(lookup(t, 1)).containsExactly(rid.toString());
    assertThat(countEntries(t)).isEqualTo(1);
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void recordDeletedAfterAKeyWasAssignedAfterTheSaveLeavesNoEntry() {
    for (final String kind : ALL_KINDS) {
      final String t = create("X", kind);
      final RID rid = insert(t, 0);

      database.transaction(() -> {
        final MutableDocument d = load(rid);
        d.set("k", 1).save();
        d.set("k", 2);
        d.delete();
      });

      for (final int key : new int[] { 0, 1, 2 })
        assertThat(lookup(t, key)).as(kind + " k=" + key).isEmpty();
      assertThat(countEntries(t)).as(kind).isZero();
    }
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void recordReloadedAfterTheSaveAndItsIndexAgree() {
    // reload() replaces the content by what the page holds, which is not what the save brought the index to: whichever value the
    // record ends up with, the index has to hold the same one
    for (final String kind : ALL_KINDS) {
      final String t = create("L", kind);
      final RID rid = insert(t, 0);

      database.transaction(() -> {
        final MutableDocument d = load(rid);
        d.set("k", 1).save();
        d.reload();
      });

      final int stored = load(rid).getInteger("k");
      assertThat(lookup(t, stored)).as(kind + " k=" + stored).containsExactly(rid.toString());
      assertThat(countEntries(t)).as(kind).isEqualTo(1);
    }
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void assignmentBeforeAStatementThatIsTakenBackIsStillIndexedAtTheCommit() {
    // The failed statement saves the record (which clears what is owed to the commit), and is then taken back: the record returns to
    // the content it held before the statement, an assignment no save had brought to the index, and the index to the earlier save
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.id INTEGER");
    database.command("sql", "CREATE PROPERTY S.k INTEGER");
    database.command("sql", "CREATE INDEX ON S (k) UNIQUE");
    database.transaction(() -> database.command("sql", "INSERT INTO S (id, k) VALUES (1, 1), (2, 2)").close());

    final RID first;
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS r FROM S WHERE id = 1")) {
      first = rs.next().<RID>getProperty("r");
    }

    database.begin();
    final MutableDocument d = load(first);
    d.set("k", 10).save();
    d.set("k", 11);
    assertThatThrownBy(() -> database.command("sql", "UPDATE S SET k = 9 WHERE id <= 2").close()).isInstanceOf(
        DuplicatedKeyException.class);
    database.commit();

    final TypeIndex index = database.getSchema().getType("S").getPolymorphicIndexByProperties("k");
    assertThat(load(first).getInteger("k")).isEqualTo(11);
    assertThat(index.get(new Object[] { 11 }).next().getIdentity()).isEqualTo(first);
    for (final int gone : new int[] { 1, 9, 10 })
      assertThat(index.get(new Object[] { gone }).hasNext()).as("k=" + gone).isFalse();
    assertThat(index.countEntries()).isEqualTo(2);
    assertCheckDatabaseFindsNoIndexDisagreement();
  }

  @Test
  void rolledBackTransactionLeavesTheIndexAsItWas() {
    final String t = create("R", "NOTUNIQUE");
    final RID rid = insert(t, 0);

    database.begin();
    final MutableDocument d = load(rid);
    d.set("k", 1).save();
    d.set("k", 2);
    database.rollback();

    assertThat(lookup(t, 0)).containsExactly(rid.toString());
    assertThat(lookup(t, 1)).isEmpty();
    assertThat(lookup(t, 2)).isEmpty();
    assertThat(countEntries(t)).isEqualTo(1);
    assertCheckDatabaseFindsNoIndexDisagreement();
  }
}
