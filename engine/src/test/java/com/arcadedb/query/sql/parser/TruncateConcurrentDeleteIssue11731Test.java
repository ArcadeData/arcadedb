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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A {@code TRUNCATE} deletes what its scan found, one record at a time. A record a concurrent transaction deleted and
 * committed after the scan found it used to fail the whole statement with a {@code RecordNotFoundException} for a record the
 * statement never named, which no caller retries (issue #11731). It is the conflict a {@code DELETE FROM} reports in the same
 * position: a retryable {@code ConcurrentModificationException}, and the retry truncates what is left.
 */
class TruncateConcurrentDeleteIssue11731Test extends TestHelper {

  @Test
  void aRecordAnotherTransactionDeletedAfterTheScanIsARetryableConflict() {
    final RID[] rids = createThree(false);

    database.begin();
    final List<Record> found = scanAll();

    // ANOTHER TRANSACTION DELETES ONE OF THE RECORDS THE SCAN FOUND AND COMMITS, BEFORE THE TRUNCATE REACHES IT
    deleteInAnotherTransaction(rids[1]);

    final RuntimeException failure = deleteAll(found);
    database.rollback();

    assertThat(failure).isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class);
    assertThat(failure.getCause()).isInstanceOf(RecordNotFoundException.class);
    assertThat(((RecordNotFoundException) failure.getCause()).getRID()).isEqualTo(rids[1]);

    // THE RETRY READS AGAIN, NO LONGER SEES THE RECORD AND DELETES THE REST
    database.begin();
    final List<Record> left = new ArrayList<>();
    database.scanType("T", false, left::add);
    assertThat(left).hasSize(2);
    assertThat(deleteAll(left)).isNull();
    database.commit();

    assertThat(countAll()).isZero();
  }

  @Test
  void truncateTypeInTheCallerTransactionRacingADeleteIsARetryableConflict() {
    statementRacingADelete(false, "TRUNCATE TYPE T UNSAFE", true);
  }

  @Test
  void truncateTypeInItsOwnTransactionRacingADeleteIsARetryableConflict() {
    statementRacingADelete(false, "TRUNCATE TYPE T UNSAFE", false);
  }

  @Test
  void truncateOfAVertexTypeRacingADeleteIsARetryableConflict() {
    statementRacingADelete(true, "TRUNCATE TYPE T UNSAFE", true);
  }

  @Test
  void truncateBucketInTheCallerTransactionRacingADeleteIsARetryableConflict() {
    statementRacingADelete(false, null, true);
  }

  @Test
  void truncateBucketInItsOwnTransactionRacingADeleteIsARetryableConflict() {
    statementRacingADelete(false, null, false);
  }

  /**
   * Runs the statement itself. The concurrent delete is committed from a before-delete listener of the first record the
   * statement deletes, on a record its scan has not reached yet: after the scan read the page, before the delete gets there.
   *
   * @param statement {@code null} for the {@code TRUNCATE BUCKET} of the bucket holding the records
   */
  private void statementRacingADelete(final boolean vertices, final String statement, final boolean inCallerTransaction) {
    final RID[] rids = createThree(vertices);
    final String sql = statement != null ?
        statement :
        "TRUNCATE BUCKET " + database.getSchema().getBucketById(rids[0].getBucketId()).getName();

    final AtomicReference<RID> victim = new AtomicReference<>();
    database.getSchema().getType("T").getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      final RID first = record.getIdentity();
      if (victim.compareAndSet(null, first.equals(rids[2]) ? rids[0] : rids[2]))
        deleteInAnotherTransaction(victim.get());
      return true;
    });

    if (inCallerTransaction)
      database.begin();
    try {
      assertThatThrownBy(() -> database.command("sql", sql)).isInstanceOf(ConcurrentModificationException.class)
          .hasMessageContaining(victim.get().toString());
    } finally {
      if (inCallerTransaction)
        database.rollback();
    }

    // NOTHING OF THE FAILED ATTEMPT IS LEFT BEHIND: THE TWO RECORDS THE CONCURRENT TRANSACTION DID NOT DELETE ARE STILL THERE
    assertThat(database.isTransactionActive()).isFalse();
    assertThat(countAll()).isEqualTo(2);

    // THE RETRY, AS THE HTTP HANDLER RUNS IT FOR A ConcurrentModificationException
    database.command("sql", sql);
    assertThat(countAll()).isZero();
  }

  @Test
  void anotherRecordFoundMissingDuringTheDeleteFailsTheTruncate() {
    final RID[] rids = createThree(false);
    final RID elsewhere = new RID(rids[0].getBucketId(), 1_000_000L);
    database.getSchema().getType("T").getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if (record.getIdentity().equals(rids[1]))
        throw new RecordNotFoundException("Record " + elsewhere + " not found", elsewhere);
      return true;
    });

    database.begin();
    final RuntimeException failure = deleteAll(scanAll());
    database.rollback();

    assertThat(failure).isInstanceOf(RecordNotFoundException.class);
    assertThat(((RecordNotFoundException) failure).getRID()).isEqualTo(elsewhere);
    assertThat(database.existsRecord(rids[1])).isTrue();
  }

  @Test
  void aListenerRefusingTheDeleteWithTheSameRidFailsTheTruncate() {
    final RID[] rids = createThree(false);
    database.getSchema().getType("T").getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if (record.getIdentity().equals(rids[1]))
        throw new RecordNotFoundException("Record " + rids[1] + " not found", rids[1]);
      return true;
    });

    database.begin();
    final RuntimeException failure = deleteAll(scanAll());
    database.rollback();

    // THE RECORD IS STILL THERE: THE EXCEPTION CAME FROM THE LISTENER, NOT FROM A CONCURRENT DELETE
    assertThat(failure).isInstanceOf(RecordNotFoundException.class);
    assertThat(((RecordNotFoundException) failure).getRID()).isEqualTo(rids[1]);
    assertThat(database.existsRecord(rids[1])).isTrue();
  }

  @Test
  void anAfterDeleteListenerFailingWithTheSameRidFailsTheTruncate() {
    final RID[] rids = createThree(false);
    database.getSchema().getType("T").getEvents().registerListener((AfterRecordDeleteListener) record -> {
      if (record.getIdentity().equals(rids[1]))
        throw new RecordNotFoundException("Record " + rids[1] + " not found", rids[1]);
    });

    database.begin();
    final RuntimeException failure = deleteAll(scanAll());
    database.rollback();

    // THE SLOT IS FREE ONLY BECAUSE THIS TRANSACTION DELETED THE RECORD BEFORE THE LISTENER RAN: NOT A CONCURRENT DELETE
    assertThat(failure).isInstanceOf(RecordNotFoundException.class);
    assertThat(((RecordNotFoundException) failure).getRID()).isEqualTo(rids[1]);
    assertThat(database.existsRecord(rids[1])).isTrue();
  }

  private RID[] createThree(final boolean vertices) {
    database.transaction(() -> {
      if (vertices)
        database.getSchema().createVertexType("T");
      else
        database.getSchema().createDocumentType("T");
    });
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      for (int i = 0; i < rids.length; i++)
        rids[i] = (vertices ? database.newVertex("T") : database.newDocument("T")).set("v", i).save().getIdentity();
    });
    return rids;
  }

  private void deleteInAnotherTransaction(final RID rid) {
    final FutureTask<Void> concurrent = new FutureTask<>(() -> database.transaction(() -> rid.getRecord().delete()), null);
    new Thread(concurrent).start();
    try {
      concurrent.get();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (final ExecutionException e) {
      throw new IllegalStateException(e.getCause());
    }
  }

  private long countAll() {
    return database.query("sql", "SELECT count(@rid) AS c FROM T").next().<Long>getProperty("c");
  }

  private List<Record> scanAll() {
    final List<Record> found = new ArrayList<>();
    database.scanType("T", false, record -> found.add(record));
    assertThat(found).hasSize(3);
    return found;
  }

  private RuntimeException deleteAll(final List<Record> found) {
    return TruncateRecordDeleter.deleteAll(database, 0, callback -> {
      for (final Record record : found)
        if (!callback.test(record))
          return;
    });
  }
}
