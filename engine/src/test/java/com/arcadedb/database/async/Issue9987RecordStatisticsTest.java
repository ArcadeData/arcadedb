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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9987: an update through {@code database.async().updateRecord(...)} did not advance the {@code updateRecord}
 * statistic the metrics endpoint publishes, because the asynchronous task re-implemented the update around the bare
 * page write and missed the counter. The guarantee is the full matrix: create, update and delete, each through the
 * synchronous and the asynchronous API, each advances its own statistic by exactly one - and an update made inside a
 * transaction is counted once, not a second time when the commit flushes it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9987RecordStatisticsTest extends TestHelper {

  @Override
  public void beginTest() {
    database.getSchema().createDocumentType("Doc");
  }

  @Test
  void synchronousCreateUpdateAndDeleteEachCountOnce() {
    final long creates = stat("createRecord");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc").set("v", 0).save().getIdentity());
    assertThat(stat("createRecord")).isEqualTo(creates + 1);

    final long updates = stat("updateRecord");
    database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().modify().set("v", 1).save());
    assertThat(stat("updateRecord")).as("counted when made, not a second time by the commit flush")
        .isEqualTo(updates + 1);

    final long deletes = stat("deleteRecord");
    database.transaction(() -> database.deleteRecord(database.lookupByRID(rid[0], true)));
    assertThat(stat("deleteRecord")).isEqualTo(deletes + 1);
  }

  @Test
  void synchronousUpdateOutsideATransactionCountsOnce() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc").set("v", 0).save().getIdentity());

    // THE PATH THAT WRITES AT ONCE IN AN IMPLICIT TRANSACTION, THROUGH updateRecordNoLock(), WHICH MUST NOT COUNT AGAIN
    final boolean autoTransaction = database.isAutoTransaction();
    database.setAutoTransaction(true);
    try {
      final long updates = stat("updateRecord");
      database.lookupByRID(rid[0], true).asDocument().modify().set("v", 1).save();
      assertThat(stat("updateRecord")).isEqualTo(updates + 1);
    } finally {
      database.setAutoTransaction(autoTransaction);
    }
    assertThat(database.lookupByRID(rid[0], true).asDocument().getInteger("v")).isEqualTo(1);
  }

  @Test
  void asynchronousCreateUpdateAndDeleteEachCountOnce() {
    final long creates = stat("createRecord");
    final AtomicInteger created = new AtomicInteger();
    database.async().createRecord(database.newDocument("Doc").set("v", 0), r -> created.incrementAndGet());
    database.async().waitCompletion();
    assertThat(created.get()).isEqualTo(1);
    assertThat(stat("createRecord")).isEqualTo(creates + 1);

    final RID rid = database.query("sql", "select from Doc").next().getIdentity().get();

    final long updates = stat("updateRecord");
    final AtomicInteger updated = new AtomicInteger();
    database.async().updateRecord(database.lookupByRID(rid, true).asDocument().modify().set("v", 1),
        r -> updated.incrementAndGet());
    database.async().waitCompletion();
    assertThat(updated.get()).isEqualTo(1);
    assertThat(stat("updateRecord")).as("the asynchronous update is counted like the synchronous one")
        .isEqualTo(updates + 1);
    assertThat(database.lookupByRID(rid, true).asDocument().getInteger("v")).isEqualTo(1);

    final long deletes = stat("deleteRecord");
    final AtomicInteger deleted = new AtomicInteger();
    database.async().deleteRecord(database.lookupByRID(rid, true), r -> deleted.incrementAndGet());
    database.async().waitCompletion();
    assertThat(deleted.get()).isEqualTo(1);
    assertThat(stat("deleteRecord")).isEqualTo(deletes + 1);
  }

  @Test
  void asynchronousUpdateWithoutACallbackIsCountedToo() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc").set("v", 0).save().getIdentity());

    final long updates = stat("updateRecord");
    database.async().updateRecord(database.lookupByRID(rid[0], true).asDocument().modify().set("v", 2), null);
    database.async().waitCompletion();

    assertThat(stat("updateRecord")).isEqualTo(updates + 1);
    assertThat(database.lookupByRID(rid[0], true).asDocument().getInteger("v")).isEqualTo(2);
  }

  @Test
  void anAsynchronousUpdateVetoedByAListenerIsNeitherCountedNorReportedAsDone() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc").set("v", 0).save().getIdentity());

    final AtomicInteger afterUpdate = new AtomicInteger();
    final BeforeRecordUpdateListener veto = record -> false;
    final AfterRecordUpdateListener after = record -> afterUpdate.incrementAndGet();
    database.getSchema().getType("Doc").getEvents().registerListener(veto).registerListener(after);
    try {
      final long updates = stat("updateRecord");
      final AtomicInteger updated = new AtomicInteger();
      final MutableDocument doc = database.lookupByRID(rid[0], true).asDocument().modify().set("v", 3);
      database.async().updateRecord(doc, r -> updated.incrementAndGet());
      database.async().waitCompletion();

      assertThat(updated.get()).as("a vetoed update does not report success").isZero();
      assertThat(afterUpdate.get()).as("nor fire the after-update listeners").isZero();
      assertThat(stat("updateRecord")).as("nor count as an update").isEqualTo(updates);
    } finally {
      database.getSchema().getType("Doc").getEvents().unregisterListener(veto).unregisterListener(after);
    }
    assertThat(database.lookupByRID(rid[0], true).asDocument().getInteger("v")).isZero();
  }

  @Test
  void anAsynchronousUpdateFiresTheAfterUpdateListenersOnce() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc").set("v", 0).save().getIdentity());

    final AtomicInteger beforeUpdate = new AtomicInteger();
    final AtomicInteger afterUpdate = new AtomicInteger();
    final BeforeRecordUpdateListener before = record -> beforeUpdate.incrementAndGet() > 0;
    final AfterRecordUpdateListener after = record -> afterUpdate.incrementAndGet();
    database.getSchema().getType("Doc").getEvents().registerListener(before).registerListener(after);
    try {
      database.async().updateRecord(database.lookupByRID(rid[0], true).asDocument().modify().set("v", 4), null);
      database.async().waitCompletion();
    } finally {
      database.getSchema().getType("Doc").getEvents().unregisterListener(before).unregisterListener(after);
    }

    assertThat(beforeUpdate.get()).isEqualTo(1);
    assertThat(afterUpdate.get()).isEqualTo(1);
  }

  private long stat(final String name) {
    return ((Number) database.getStats().get(name)).longValue();
  }
}
