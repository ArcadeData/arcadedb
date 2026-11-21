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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.ConsistentKeyLookup;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The entries an older build wrote under the old spelling of a list or a number (issues #12160 and #12233) are read only on an index
 * that may still hold them: one whose schema entry an older build wrote, until {@code REBUILD INDEX}. An index this build created
 * holds canonical keys only, and a seek, a removal and a uniqueness check on it read no old spelling and load no record (PR #12166
 * review).
 * <p>
 * The tests write the entries an older build wrote by hand, and some leave entries no record holds on purpose, so the closing CHECK
 * DATABASE of {@link TestHelper} is off: the tests that end on a consistent index check it themselves.
 */
class LSMTreeIndexLegacyKeysTest extends TestHelper {

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  @Test
  void anIndexThisBuildCreatedHoldsCanonicalKeysAcrossARestart() {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    assertThat(mayHoldLegacyKeys("U[v]")).isNotEmpty().containsOnly(false);
    reopenDatabase();
    assertThat(mayHoldLegacyKeys("U[v]")).isNotEmpty().containsOnly(false);
  }

  /**
   * The guard: an index this build created reads no old spelling and loads no record. Entries no write of this build makes are put in
   * by hand, only to see that nothing looks for them: the old spelling of a list, and the STRING '3.5' for a record holding 7. Once
   * the same index reads as written by an older build, a seek reads the old spelling and drops the holders that do not hold the key.
   */
  @Test
  void aSeekOnAnIndexThisBuildCreatedReadsNoOldSpellingAndLoadsNoRecord() throws IOException {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    final RID list = create("U", 0, List.of(1.5, 2.0));
    final RID number = create("U", 1, 7L);
    replaceEntry("U[v]", list, List.of(1.5, 2.0), "[1.5, 2.0]");
    addEntry("U[v]", number, "3.5");

    // no read of the old spelling: the list is not found under it
    assertThat(seek("U[v]", List.of(1.5, 2.0))).isEmpty();
    // no record loaded: the entries of a STRING come back as stored, those of a record that does not hold it included
    assertThat(seek("U[v]", "[1.5, 2.0]")).containsExactly(list);
    assertThat(seek("U[v]", "3.5")).containsExactly(number);
    assertThat(seek("U[v]", "1e5")).isEmpty();

    reopenAsAnOlderBuildLeftIt();
    assertThat(mayHoldLegacyKeys("U[v]")).isNotEmpty().containsOnly(true);
    assertThat(seek("U[v]", List.of(1.5, 2.0))).containsExactly(list);
    assertThat(seek("U[v]", "[1.5, 2.0]")).isEmpty();
    assertThat(seek("U[v]", "3.5")).isEmpty();
  }

  /** The guard on the commit path: a unique index this build created looks for no copy under an old spelling. */
  @Test
  void aUniqueIndexThisBuildCreatedChecksNoOldSpellingAtCommit() throws IOException {
    database.command("opencypher", "CREATE CONSTRAINT FOR (n:K) REQUIRE n.v IS UNIQUE");
    final RID holder = create("K", 0, 1.0);
    replaceEntry("K[v]", holder, 1.0, "1.0");

    // the entry under '1.0' is never looked at, nor its record loaded: the copy goes in
    final RID copy = create("K", 1, 1.0);
    database.transaction(() -> database.deleteRecord(database.lookupByRID(copy, true)));

    reopenAsAnOlderBuildLeftIt();
    assertThatThrownBy(() -> create("K", 2, 1.0)).isInstanceOf(DuplicatedKeyException.class);
    // a numeric-looking STRING is another value than the number, under either spelling
    create("K", 3, "1.0");
    create("K", 4, "12345");
  }

  /** The guard on the removal path: an update on an index this build created removes the canonical entry alone. */
  @Test
  void aRemovalOnAnIndexThisBuildCreatedReadsNoOldSpelling() throws IOException {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    final RID first = create("U", 0, List.of(1.0));
    final RID second = create("U", 1, List.of(2.0));
    replaceEntry("U[v]", first, List.of(1.0), "[1.0]");
    replaceEntry("U[v]", second, List.of(2.0), "[2.0]");

    // the entry under '[1.0]' is never looked at, so it stays: the update leaves '[1.0]' and adds '[9]'
    database.transaction(() -> database.command("opencypher", "MATCH (n:U {id: 0}) SET n.v = [9]"));
    assertThat(database.getSchema().getIndexByName("U[v]").countEntries()).isEqualTo(3L);

    reopenAsAnOlderBuildLeftIt();
    // the entry under '[2.0]' goes with the update of its record
    database.transaction(() -> database.command("opencypher", "MATCH (n:U {id: 1}) SET n.v = [8]"));
    assertThat(database.getSchema().getIndexByName("U[v]").countEntries()).isEqualTo(3L);
    assertThat(seek("U[v]", List.of(2.0))).isEmpty();
    assertThat(seek("U[v]", List.of(8))).containsExactly(second);
  }

  /** REBUILD INDEX writes every entry under its canonical key, and the index stops reading the old spellings, after a restart too. */
  @Test
  void rebuildIndexMarksTheKeysCanonicalForGood() throws IOException {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    final RID list = create("U", 0, List.of(1.5, 2.0));
    replaceEntry("U[v]", list, List.of(1.5, 2.0), "[1.5, 2.0]");
    reopenAsAnOlderBuildLeftIt();
    assertThat(mayHoldLegacyKeys("U[v]")).isNotEmpty().containsOnly(true);

    database.command("sql", "REBUILD INDEX `U[v]`");
    assertThat(mayHoldLegacyKeys("U[v]")).isNotEmpty().containsOnly(false);
    assertThat(seek("U[v]", List.of(1.5, 2.0))).containsExactly(list);
    assertThat(seek("U[v]", List.of(1.5, 2))).containsExactly(list);

    reopenDatabase();
    assertThat(mayHoldLegacyKeys("U[v]")).isNotEmpty().containsOnly(false);
    assertThat(seek("U[v]", List.of(1.5, 2.0))).containsExactly(list);
    assertThat(checkDatabaseErrors()).isZero();
  }

  /**
   * A seek reads the old key and then the canonical one. A commit between the two reads that moves the record's entry from the old key
   * to the canonical one ([1.0] rewritten as [1], one key) must not hide the record from the seek.
   */
  @Test
  void aCommitBetweenTheTwoReadsOfASeekHidesNoRecord() throws IOException {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    final RID rid = create("U", 0, List.of(1.0));
    replaceEntry("U[v]", rid, List.of(1.0), "[1.0]");
    reopenAsAnOlderBuildLeftIt();

    final AtomicBoolean moved = new AtomicBoolean();
    LSMTreeIndexLegacyKeys.setBetweenReadsTestHook(() -> {
      if (moved.compareAndSet(false, true))
        runInAnotherThread(() -> database.transaction(
            () -> database.command("opencypher", "MATCH (n:U {id: 0}) SET n.v = $v", Map.of("v", List.of(1L)))));
    });
    try {
      assertThat(seek("U[v]", List.of(1.0))).containsExactly(rid);
      assertThat(moved.get()).isTrue();
    } finally {
      LSMTreeIndexLegacyKeys.setBetweenReadsTestHook(null);
    }
    assertThat(checkDatabaseErrors()).isZero();
  }

  /** A record deleted between the two reads of a seek, after its old entry was read, is not answered, and the seek does not fail. */
  @Test
  void aRecordDeletedDuringASeekIsNotAnswered() throws IOException {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    final RID rid = create("U", 0, List.of(1.0));
    replaceEntry("U[v]", rid, List.of(1.0), "[1.0]");
    reopenAsAnOlderBuildLeftIt();

    final AtomicBoolean deleted = new AtomicBoolean();
    LSMTreeIndexLegacyKeys.setBetweenReadsTestHook(() -> {
      if (deleted.compareAndSet(false, true))
        runInAnotherThread(() -> database.transaction(() -> database.command("opencypher", "MATCH (n:U {id: 0}) DELETE n")));
    });
    try {
      assertThat(seek("U[v]", List.of(1.0))).isEmpty();
      assertThat(deleted.get()).isTrue();
    } finally {
      LSMTreeIndexLegacyKeys.setBetweenReadsTestHook(null);
    }
    assertThat(database.getSchema().getIndexByName("U[v]").countEntries()).isZero();
  }

  /**
   * The entries a seek meets when their record was deleted or changed after the entry was read: one of a record that is gone, whose
   * load throws RecordNotFoundException, and one of a record that holds another value now. Neither holds the key, for the seek of the
   * list and for the seek of a STRING spelling an old key.
   */
  @Test
  void anOldEntryWhoseRecordIsGoneOrChangedHoldsNoKey() throws IOException {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    final RID gone = create("U", 0, List.of(1.0));
    final RID changed = create("U", 1, List.of(7));
    database.transaction(() -> database.deleteRecord(database.lookupByRID(gone, true)));
    for (final RID rid : List.of(gone, changed)) {
      addEntry("U[v]", rid, "[1.0]");
      addEntry("U[v]", rid, "1.5");
    }
    reopenAsAnOlderBuildLeftIt();

    assertThat(seek("U[v]", List.of(1.0))).isEmpty();
    assertThat(seek("U[v]", "1.5")).isEmpty();
    assertThat(seek("U[v]", List.of(7))).containsExactly(changed);
  }

  /**
   * A case-insensitive index folds a key to lower case, and so did the older build: 1e300 was keyed '1.0e300', the folded
   * {@code Double.toString()}. The seek, the removal and CHECK DATABASE look under the folded old spelling, and a STRING that folds to
   * it is another value.
   */
  @Test
  void anOldNumberKeyOfACaseInsensitiveIndexIsFoundFolded() throws IOException {
    database.getSchema().buildDocumentType().withName("F").withTotalBuckets(1).create();
    database.getSchema().buildTypeIndex("F", new String[] { "v" })
        .withDefaultKeyTypesForUndeclaredProperties(new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withCollations(List.of("CI"))
        .withUnique(false)
        .create();
    final RID number = createDocument("F", 0, 1e300);
    final RID list = createDocument("F", 1, List.of(1e300));
    final RID string = createDocument("F", 2, "1.0E300");
    replaceEntry("F[v]", number, 1e300, "1.0E300");
    replaceEntry("F[v]", list, List.of(1e300), "[1.0E300]");
    reopenAsAnOlderBuildLeftIt();
    assertThat(checkDatabaseErrors()).isZero();

    assertThat(seek("F[v]", 1e300)).containsExactly(number);
    assertThat(seek("F[v]", List.of(1e300))).containsExactly(list);
    assertThat(seek("F[v]", "1.0E300")).containsExactly(string);
    assertThat(seek("F[v]", "1.0e300")).containsExactly(string);

    database.transaction(() -> {
      database.deleteRecord(database.lookupByRID(number, true));
      database.deleteRecord(database.lookupByRID(list, true));
    });
    assertThat(database.getSchema().getIndexByName("F[v]").countEntries()).isEqualTo(1L);
    assertThat(checkDatabaseErrors()).isZero();
  }

  /**
   * A FLOAT and a DECIMAL were keyed by their {@code toString()} before #9132 too, as a DOUBLE was: 1.0f as '1.0', -0.0f as '-0.0' and
   * 2.500 as '2.500', where the canonical keys are '1', '0' and '2.5'.
   */
  @Test
  void oldFloatAndDecimalKeysAreFoundRefusedAndRemoved() throws IOException {
    database.command("opencypher", "CREATE CONSTRAINT FOR (n:N) REQUIRE n.v IS UNIQUE");
    final List<Object> values = List.of(1.0f, -0.0f, new BigDecimal("2.500"));
    final List<RID> rids = new ArrayList<>();
    for (int i = 0; i < values.size(); i++) {
      rids.add(create("N", i, values.get(i)));
      replaceEntry("N[v]", rids.get(i), values.get(i), values.get(i).toString());
    }
    reopenAsAnOlderBuildLeftIt();
    assertThat(checkDatabaseErrors()).isZero();

    for (int i = 0; i < values.size(); i++) {
      final Object value = values.get(i);
      assertThat(seek("N[v]", value)).as("seek %s", value).containsExactly(rids.get(i));
      final int id = 10 + i;
      assertThatThrownBy(() -> create("N", id, value)).as("copy of %s", value).isInstanceOf(DuplicatedKeyException.class);
    }

    database.transaction(() -> {
      for (final RID rid : rids)
        database.deleteRecord(database.lookupByRID(rid, true));
    });
    assertThat(database.getSchema().getIndexByName("N[v]").countEntries()).isZero();
    assertThat(checkDatabaseErrors()).isZero();
  }

  /**
   * The marker REBUILD INDEX sets reaches the schema the way a replicated schema change does, inside a schema recording session: under
   * HA only a session ships the schema to the followers, and a bare save wrote the leader's file alone, so a follower (and a follower
   * promoted to leader) went on reading the old spellings. The session is observable without a cluster through
   * {@code SCHEMA_AFTER_FILE_CHANGES}, which fires at the end of every outermost session and only there.
   */
  @Test
  void rebuildIndexRecordsTheCanonicalMarkerInASchemaSession() throws IOException {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    final RID list = create("U", 0, List.of(1.5, 2.0));
    replaceEntry("U[v]", list, List.of(1.5, 2.0), "[1.5, 2.0]");
    reopenAsAnOlderBuildLeftIt();
    assertThat(mayHoldLegacyKeys("U[v]")).isNotEmpty().containsOnly(true);

    final List<Boolean> canonicalAtTheEndOfASession = new ArrayList<>();
    final Callable<Void> observer = () -> {
      canonicalAtTheEndOfASession.add(!mayHoldLegacyKeys("U[v]").contains(true));
      return null;
    };
    ((DatabaseInternal) database).registerCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, observer);
    try {
      database.command("sql", "REBUILD INDEX `U[v]`");
    } finally {
      ((DatabaseInternal) database).unregisterCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, observer);
    }

    assertThat(canonicalAtTheEndOfASession).isNotEmpty();
    // the last session of the rebuild, the one that ships the schema last, already carries the marker
    assertThat(canonicalAtTheEndOfASession.get(canonicalAtTheEndOfASession.size() - 1)).isTrue();
  }

  /**
   * A removal of a key as a whole inside a transaction takes the entries under the old spelling of the value with it, and only those:
   * the old text is also the key of a STRING, whose entry stays.
   */
  @Test
  void aKeyWideRemovalInATransactionTakesTheOldEntriesAndNotTheStrings() throws IOException {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    final RID list = create("U", 0, List.of(1.5, 2.0));
    final RID string = create("U", 1, "[1.5, 2.0]");
    replaceEntry("U[v]", list, List.of(1.5, 2.0), "[1.5, 2.0]");
    reopenAsAnOlderBuildLeftIt();
    assertThat(seek("U[v]", List.of(1.5, 2.0))).containsExactly(list);

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("U[v]");
    database.transaction(() -> {
      for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
        bucketIndex.remove(new Object[] { List.of(1.5, 2.0) });
    });

    assertThat(seek("U[v]", List.of(1.5, 2.0))).isEmpty();
    assertThat(seek("U[v]", "[1.5, 2.0]")).containsExactly(string);
    assertThat(typeIndex.countEntries()).isEqualTo(1L);
  }

  /**
   * A seek in a REPEATABLE_READ transaction on an index that may hold old entries is served while commits publish around it. The two reads
   * of a seek run between two samples of the publication sequence, and a read that overlapped a commit is repeated a bounded number of
   * times and then read against one commit: a transaction that pins pages must not make the seek spin, nor fail it. Here a commit
   * overlaps every attempt that can be overlapped.
   */
  @Test
  void aSeekInARepeatableReadTransactionOnAnOldIndexIsServedWhileCommitsOverlapIt() throws IOException {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    final RID list = create("U", 0, List.of(1.5, 2.0));
    replaceEntry("U[v]", list, List.of(1.5, 2.0), "[1.5, 2.0]");
    reopenAsAnOlderBuildLeftIt();

    final AtomicInteger reads = new AtomicInteger();
    // the last attempt runs with the publications held off: a commit waiting for them from the hook would wait for the seek
    LSMTreeIndexLegacyKeys.setBetweenReadsTestHook(() -> {
      final int read = reads.getAndIncrement();
      if (read < ConsistentKeyLookup.MAX_ATTEMPTS)
        runInAnotherThread(() -> create("U", 100 + read, 5L));
    });
    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(seek("U[v]", List.of(1.5, 2.0))).containsExactly(list);
      assertThat(reads.get()).isGreaterThan(1).isLessThanOrEqualTo(ConsistentKeyLookup.MAX_ATTEMPTS + 1);
    } finally {
      database.rollback();
      LSMTreeIndexLegacyKeys.setBetweenReadsTestHook(null);
    }
  }

  /**
   * An index file whose entry in schema.json names another file than the one it has now (a compaction renamed the file after the schema
   * was saved) is linked back to its entry by the bucket it indexes. That entry names the type where every other load reads the kind of
   * the index, and the link must still tell an LSM_TREE index an older build wrote from one of another kind: it goes on reading the old
   * spellings, and the next save does not write the marker of an index whose keys are all canonical over it.
   */
  @Test
  void anIndexLinkedBackToItsEntryByItsBucketKeepsReadingTheOldSpellings() throws IOException {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    final RID list = create("U", 0, List.of(1.5, 2.0));
    replaceEntry("U[v]", list, List.of(1.5, 2.0), "[1.5, 2.0]");
    database.close();
    LegacyKeyIndexes.forgetCanonicalKeysAndFileNames(getDatabasePath());
    database = factory.open();

    assertThat(mayHoldLegacyKeys("U[v]")).isNotEmpty().containsOnly(true);
    assertThat(seek("U[v]", List.of(1.5, 2.0))).containsExactly(list);
    assertThat(seek("U[v]", List.of(1.5, 2))).isEmpty();
  }

  /**
   * A seek of a STRING and a uniqueness check load the records of the entries under a text only when an older build could have written
   * that text for a list or a number. A version, an address, a timestamp or a unit are none of them: on an index an older build wrote, the
   * entries under them come back as stored and no record is loaded (PR #12166 review).
   */
  @Test
  void aSeekOfATextNoOldBuildWroteForAListOrANumberLoadsNoRecordOnAnOldIndex() throws IOException {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    final RID rid = create("U", 0, 7L);
    final List<String> texts = List.of("3.1.4", "10.0.0.1", "2020-01-02T10:00:00.123Z", "1.5 kg", "1e", "1.", "-1.2.3");
    for (final String text : texts)
      addEntry("U[v]", rid, text);
    reopenAsAnOlderBuildLeftIt();
    assertThat(mayHoldLegacyKeys("U[v]")).isNotEmpty().containsOnly(true);

    // no record is loaded: the entries, which the record does not hold, come back as stored
    for (final String text : texts)
      assertThat(seek("U[v]", text)).as("seek %s", text).containsExactly(rid);

    // the text of a number an old build could have written is held to the record, which holds 7
    addEntry("U[v]", rid, "3.50");
    assertThat(seek("U[v]", "3.50")).isEmpty();
  }

  /** The texts the two guards of a STRING key let through: the ones an older build wrote, or could have, for a list or a number. */
  @Test
  void onlyATextOfAListOrANumberIsLookedForUnderItsOldSpelling() {
    database.command("opencypher", "CREATE INDEX FOR (n:U) ON (n.v)");
    final LSMTreeIndexMutable keys = ((LSMTreeIndex) ((TypeIndex) database.getSchema().getIndexByName("U[v]")).getIndexesOnBuckets()[0]).mutable;

    // the old text of a list or a number differs from the canonical one by a point or an exponent
    for (final String text : List.of("1.0", "-0.0", "1.0E300", "1.0e300", "1e+3", "-1.5E-7", "2.500", "0.5", "[1.5, 2.0]", "[1.5, a]"))
      assertThat(keys.maySpellALegacyKey(new Object[] { text }, new Object[] { text })).as("old spelling of %s", text).isTrue();
    for (final String text : List.of("3.1.4", "10.0.0.1", "2020-01-02T10:00:00.123Z", "1.5 kg", "1e", "1e+", "1.", ".5", "-", "12345", "-7",
        "v1.0", "abc", "", "[x]", "[1.5, 2.0", "1.0]"))
      assertThat(keys.maySpellALegacyKey(new Object[] { text }, new Object[] { text })).as("no old spelling of %s", text).isFalse();
    // a key that is no STRING (a number, a list) is looked for by its value, not by its text
    assertThat(keys.maySpellALegacyKey(new Object[] { 1.0 }, new Object[] { "1" })).isFalse();

    // the canonical key of a list or a number, the texts whose record can hold such a value
    for (final String text : List.of("1", "12345", "-5", "2.5", "0", "1E+500", "[1.5, 2]", "[a]"))
      assertThat(keys.mayBeCanonicalKeyOfLegacyValue(new Object[] { text })).as("canonical key of %s", text).isTrue();
    for (final String text : List.of("3.1.4", "2020-01-02", "555-1234", "1st", "1.", ".5", "-", "v2", "", "[x", "abc", "1e", "10.0.0.1"))
      assertThat(keys.mayBeCanonicalKeyOfLegacyValue(new Object[] { text })).as("no canonical key of %s", text).isFalse();
  }

  private RID create(final String typeName, final int id, final Object value) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newVertex(typeName).set("id", id).set("v", value).save().getIdentity());
    return rid[0];
  }

  private RID createDocument(final String typeName, final int id, final Object value) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(typeName).set("id", id).set("v", value).save().getIdentity());
    return rid[0];
  }

  /** Replaces, in the bucket index of {@code rid}, the entry of {@code value} with the one an older build wrote: {@code spelling}. */
  private void replaceEntry(final String indexName, final RID rid, final Object value, final String spelling) {
    database.transaction(() -> {
      final IndexInternal bucketIndex = bucketIndexOf(indexName, rid);
      bucketIndex.remove(new Object[] { value }, rid);
      bucketIndex.put(new Object[] { spelling }, new RID[] { rid });
    });
  }

  private void addEntry(final String indexName, final RID rid, final String key) {
    database.transaction(() -> bucketIndexOf(indexName, rid).put(new Object[] { key }, new RID[] { rid }));
  }

  private IndexInternal bucketIndexOf(final String indexName, final RID rid) {
    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets())
      if (bucketIndex.getAssociatedBucketId() == rid.getBucketId())
        return bucketIndex;
    throw new IllegalStateException("No bucket index of " + indexName + " for " + rid);
  }

  private List<Boolean> mayHoldLegacyKeys(final String indexName) {
    final List<Boolean> flags = new ArrayList<>();
    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets())
      flags.add(((LSMTreeIndex) bucketIndex).mayHoldLegacyKeys());
    return flags;
  }

  private List<RID> seek(final String indexName, final Object key) {
    final List<RID> found = new ArrayList<>();
    try (final IndexCursor cursor = database.getSchema().getIndexByName(indexName).get(new Object[] { key })) {
      while (cursor.hasNext())
        found.add(cursor.next().getIdentity());
    }
    return found;
  }

  /** Reopens the database with the schema entries an older build writes: without the marker of an index whose keys are all canonical. */
  private void reopenAsAnOlderBuildLeftIt() throws IOException {
    database.close();
    LegacyKeyIndexes.forgetCanonicalKeys(getDatabasePath());
    database = factory.open();
  }

  private static void runInAnotherThread(final Runnable task) {
    final Thread thread = new Thread(task);
    thread.start();
    try {
      thread.join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private long checkDatabaseErrors() {
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      return rs.next().<Number>getProperty("totalErrors").longValue();
    }
  }
}
