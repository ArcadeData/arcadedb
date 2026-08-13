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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code CHECK DATABASE FIX} over a plain DOCUMENT type holding an unreadable record.
 * <p>
 * The two halves below were one defect in practice: {@code DatabaseChecker.checkDocuments} flagged a corrupted
 * document through {@code addCorrupted} but - unlike the vertex and edge arms in {@code GraphDatabaseChecker} -
 * never DELETED it, while the shared tail of {@code check()} still put its bucket into {@code affectedBuckets} and
 * dropped and rebuilt every index there. The rebuild's own bucket scan then met the same unreadable record,
 * {@code LSMTreeIndex.build}'s error callback propagated it (deliberately - see its comment), and because the
 * rebuild loop caught only {@code NeedRetryException} the failure escaped {@code check()} entirely with that
 * attempt's {@code dropIndex()} already committed. Net effect on a single node, no cluster involved: running
 * {@code CHECK DATABASE FIX} to repair one corrupt document DESTROYED the type's index and aborted the whole
 * check, so nothing after it was repaired either.
 * <p>
 * Every {@code CHECK DATABASE FIX} index test that existed before this one used {@code CREATE VERTEX TYPE} (see
 * {@code CheckDatabaseFixPreservesIndexMetadataTest}), where the graph arm deletes the record first and the
 * rebuild therefore never meets it - which is why the document arm's omission survived.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseFixDocumentTypeTest extends TestHelper {

  /**
   * The primary half: a corrupted document is removed, so the rebuild that follows has nothing to trip over and
   * the index survives with the entries of the records that are still readable.
   */
  @Test
  void fixDeletesCorruptedDocumentAndPreservesIndex() {
    final RID victim = createIndexedDocs("myDocIdx", "NOTUNIQUE");

    // PRECONDITION: both records are indexed before anything is broken, so a later count of 1 means the rebuild
    // dropped exactly the corrupted one rather than the index having been short all along.
    assertThat(database.getSchema().getIndexByName("myDocIdx").countEntries()).isEqualTo(2L);

    corruptRecordTypeByte((DatabaseInternal) database, victim);

    final Result fix;
    try (final ResultSet rs = database.command("sql", "check database fix")) {
      fix = rs.next();
    }

    assertThat(fix.<Long>getProperty("autoFix"))
        .as("the corrupted document must be counted as repaired, exactly as the vertex arm counts its own")
        .isGreaterThan(0L);

    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("myDocIdx"))
          .as("the index must survive: the rebuild had nothing left to trip over").isTrue();
      assertThat(database.getSchema().getIndexByName("myDocIdx").getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
      assertThat(database.getSchema().getIndexByName("myDocIdx").countEntries())
          .as("the rebuilt index holds the surviving record only").isEqualTo(1L);

      assertThat(database.countType("Doc", false)).as("the corrupted document is gone").isEqualTo(1L);
    });

    // A follow-up check, WITHOUT fix, must find nothing: a run that reported a repair it did not make would
    // otherwise pass everything above.
    try (final ResultSet rs = database.command("sql", "check database")) {
      final Result verify = rs.next();
      assertThat(verify.<Long>getProperty("totalCorruptedRecords")).isZero();
      assertThat(verify.<Long>getProperty("totalWarnings")).isZero();
    }
  }

  /** The same repair through the {@code RECORD} scope, which reaches the corrupted document by its own arm. */
  @Test
  void recordScopedFixDeletesCorruptedDocumentAndPreservesIndex() {
    final AtomicReference<RID> victim = new AtomicReference<>(createIndexedDocs("myDocIdx", "NOTUNIQUE"));

    assertThat(database.getSchema().getIndexByName("myDocIdx").countEntries()).isEqualTo(2L);

    corruptRecordTypeByte((DatabaseInternal) database, victim.get());

    try (final ResultSet rs = database.command("sql", "check database record " + victim.get() + " fix")) {
      assertThat(rs.next().<Long>getProperty("autoFix")).isGreaterThan(0L);
    }

    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("myDocIdx")).isTrue();
      assertThat(database.getSchema().getIndexByName("myDocIdx").countEntries()).isEqualTo(1L);
      assertThat(database.countType("Doc", false)).isEqualTo(1L);
    });
  }

  /**
   * The second half, which the first does not cover: a rebuild that fails for a reason the checker cannot remove
   * first. The retry loop only ever caught {@code NeedRetryException}, so anything else - a
   * {@code DuplicatedKeyException} here, a {@code ReplicatedEntryTooLargeException} on an HA leader whose index is
   * bigger than {@code arcadedb.ha.appendBufferSize} - unwound out of {@code check()} with the drop already
   * committed, discarding every repair the run had made so far and leaving the index missing with no report of it.
   * <p>
   * The duplicate is inserted straight into the bucket via {@code LocalBucket.createRecord}, which does not go
   * through {@code DocumentIndexer}: that is the only way to hold a unique index and a violating record at the same
   * time, which is exactly the state a rebuild cannot resolve.
   */
  @Test
  void rebuildFailureIsReportedAndDoesNotAbortTheCheck() {
    final AtomicReference<RID> victim = new AtomicReference<>();
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.name STRING");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Doc SET name = 'alpha'");
      victim.set(database.command("sql", "INSERT INTO Doc SET name = 'gamma'").next().toElement().getIdentity());
    });
    database.command("sql", "CREATE INDEX myUniqueDocIdx ON Doc (name) UNIQUE");
    assertThat(database.getSchema().getIndexByName("myUniqueDocIdx").countEntries()).isEqualTo(2L);

    // A second 'alpha' the unique index knows nothing about. Legal to write, impossible to index.
    database.transaction(() -> {
      final MutableDocument duplicate = database.newDocument("Doc").set("name", "alpha");
      database.getSchema().getBucketById(victim.get().getBucketId()).createRecord(duplicate, false);
    });

    // The corrupted record is what puts the bucket into affectedBuckets and so triggers the rebuild at all.
    corruptRecordTypeByte((DatabaseInternal) database, victim.get());

    final Result fix;
    try (final ResultSet rs = database.command("sql", "check database fix")) {
      assertThat(rs.hasNext()).as("the check must COMPLETE and return its report, not unwind").isTrue();
      fix = rs.next();
    }

    assertThat(fix.<Long>getProperty("autoFix"))
        .as("the repairs made before the failing index must be kept, not discarded with it").isGreaterThan(0L);

    final Collection<String> rebuilt = fix.getProperty("rebuiltIndexes");
    assertThat(rebuilt)
        .as("an index that did not rebuild must not be claimed as rebuilt")
        .doesNotContain("myUniqueDocIdx");

    final Collection<String> warnings = fix.getProperty("warnings");
    assertThat(warnings)
        .as("the failure must be REPORTED, naming the index and saying it is now missing")
        .anyMatch(w -> w.contains("myUniqueDocIdx") && w.contains("missing"));

    // The honest end state: the index really is gone. Recreating it empty would be worse - queries would silently
    // return nothing - so the contract is that the operator is told, loudly, and recreates it.
    assertThat(database.getSchema().existsIndex("myUniqueDocIdx")).isFalse();
  }

  /**
   * Two documents plus an index over them. The insert and the {@code CREATE INDEX} are deliberately in SEPARATE
   * transactions: {@code BucketIndexBuilder.create()} builds in a transaction of its own with
   * {@code joinCurrentTx = false}, so an index created inside the transaction that inserted the records is built
   * against committed state and comes out EMPTY - which would make every entry-count assertion below vacuous.
   *
   * @return the RID of the record the caller is meant to corrupt
   */
  private RID createIndexedDocs(final String indexName, final String uniqueness) {
    final AtomicReference<RID> victim = new AtomicReference<>();
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.name STRING");
    database.transaction(() -> {
      victim.set(database.command("sql", "INSERT INTO Doc SET name = 'alpha'").next().toElement().getIdentity());
      database.command("sql", "INSERT INTO Doc SET name = 'beta'");
    });
    database.command("sql", "CREATE INDEX " + indexName + " ON Doc (name) " + uniqueness);
    return victim.get();
  }
}
