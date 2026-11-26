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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A record {@code scanType} or {@code scanBucket} hands out was read from its bucket in the current transaction, as one
 * {@code iterateType} or {@code lookupByRID} hands out is, and must say so (issue #12575). Without the read marker, deleting
 * a scanned record that a concurrent transaction deleted and committed after the scan raised the raw, non-retryable
 * {@link RecordNotFoundException} instead of the retryable {@link ConcurrentModificationException} of #8984, and a vertex
 * found by a scan and modified after a concurrent update overwrote that update instead of being refused (#8610).
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ScanReadMarkerIssue12575Test extends TestHelper {

  @Test
  void deletingARecordFoundByScanTypeAfterAConcurrentDeleteIsARetryableConflict() {
    final RID[] rids = createThree(false);

    database.begin();
    try {
      final List<Record> found = new ArrayList<>();
      database.scanType("T", false, found::add);
      assertThat(found).hasSize(3);

      deleteInAnotherTransaction(rids[1]);

      assertConcurrentDelete(recordOf(found, rids[1]), rids[1]);
    } finally {
      database.rollback();
    }
  }

  @Test
  void deletingARecordFoundByScanBucketAfterAConcurrentDeleteIsARetryableConflict() {
    final RID[] rids = createThree(false);
    final String bucketName = database.getSchema().getBucketById(rids[0].getBucketId()).getName();

    database.begin();
    try {
      final List<Record> found = new ArrayList<>();
      database.scanBucket(bucketName, found::add);
      assertThat(found).hasSize(3);

      deleteInAnotherTransaction(rids[1]);

      assertConcurrentDelete(recordOf(found, rids[1]), rids[1]);
    } finally {
      database.rollback();
    }
  }

  @Test
  void deletingAVertexFoundByScanTypeAfterAConcurrentDeleteIsARetryableConflict() {
    final RID[] rids = createThree(true);

    database.begin();
    try {
      final List<Record> found = new ArrayList<>();
      database.scanType("T", false, found::add);
      assertThat(found).hasSize(3);

      deleteInAnotherTransaction(rids[2]);

      assertConcurrentDelete(recordOf(found, rids[2]), rids[2]);
    } finally {
      database.rollback();
    }
  }

  /** The reference behaviour the scans now agree with. */
  @Test
  void deletingARecordFoundByIterateTypeAfterAConcurrentDeleteIsARetryableConflict() {
    final RID[] rids = createThree(false);

    database.begin();
    try {
      final List<Record> found = new ArrayList<>();
      final Iterator<Record> it = database.iterateType("T", false);
      while (it.hasNext())
        found.add(it.next());
      assertThat(found).hasSize(3);

      deleteInAnotherTransaction(rids[1]);

      assertConcurrentDelete(recordOf(found, rids[1]), rids[1]);
    } finally {
      database.rollback();
    }
  }

  @Test
  void aRecordScannedInAnEarlierTransactionKeepsTheNotFound() {
    final RID[] rids = createThree(false);

    final List<Record> found = new ArrayList<>();
    database.transaction(() -> database.scanType("T", false, found::add));
    assertThat(found).hasSize(3);

    deleteInAnotherTransaction(rids[0]);

    // READ IN ANOTHER TRANSACTION: NOTHING SAYS THIS ONE SAW THE RECORD, SO THE RID NAMES A RECORD THAT IS GONE FOR GOOD
    database.begin();
    try {
      final Record record = recordOf(found, rids[0]);
      assertThatThrownBy(record::delete).isInstanceOf(RecordNotFoundException.class);
    } finally {
      database.rollback();
    }
  }

  @Test
  void modifyingAVertexFoundByScanTypeAfterAConcurrentUpdateIsRefused() {
    final RID[] rids = createThree(true);

    database.begin();
    try {
      final List<Record> found = new ArrayList<>();
      database.scanType("T", false, found::add);
      final Vertex vertex = (Vertex) recordOf(found, rids[0]);
      final int readValue = vertex.getInteger("v");

      runInAnotherTransaction(() -> rids[0].asVertex().modify().set("v", 100).save());

      // A WRITE COMPUTED FROM THE OLDER READ WOULD OVERWRITE THE CONCURRENT CHANGE (#8610): IT IS REFUSED AS A RETRYABLE CONFLICT
      assertThatThrownBy(() -> vertex.modify().set("v", readValue + 1).save()).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      database.rollback();
    }

    assertThat(database.query("sql", "SELECT v FROM " + rids[0]).next().<Integer>getProperty("v")).isEqualTo(100);
  }

  @Test
  void aScanThatFindsNothingConcurrentDeletesAndModifiesNormally() {
    final RID[] rids = createThree(true);

    database.transaction(() -> database.scanType("T", false, record -> {
      if (record.getIdentity().equals(rids[0]))
        record.delete();
      else
        ((Vertex) record).modify().set("v", 10).save();
      return true;
    }));

    assertThat(database.countType("T", false)).isEqualTo(2);
    assertThat(database.query("sql", "SELECT FROM T WHERE v = 10").stream().count()).isEqualTo(2);
  }

  private static void assertConcurrentDelete(final Record record, final RID rid) {
    assertThatThrownBy(record::delete).isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class)
        .satisfies(e -> {
          assertThat(e.getCause()).isInstanceOf(RecordNotFoundException.class);
          assertThat(((RecordNotFoundException) e.getCause()).getRID()).isEqualTo(rid);
        });
  }

  private static Record recordOf(final List<Record> found, final RID rid) {
    for (final Record record : found)
      if (record.getIdentity().equals(rid))
        return record;
    throw new AssertionError("Record " + rid + " not found by the scan");
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
    runInAnotherTransaction(() -> rid.getRecord().delete());
  }

  private void runInAnotherTransaction(final Runnable work) {
    final FutureTask<Void> concurrent = new FutureTask<>(() -> database.transaction(work::run), null);
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
}
