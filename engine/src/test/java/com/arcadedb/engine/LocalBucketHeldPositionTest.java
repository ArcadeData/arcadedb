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
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12336, at the bucket: a held position is never handed to a new record, by any transaction, whether the commit left
 * it a hole inside the record table or trimmed it off the end, and it is reused as soon as the hold is released.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LocalBucketHeldPositionTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create();
  }

  private LocalBucket bucket() {
    return (LocalBucket) database.getSchema().getType("Doc").getBuckets(false).getFirst();
  }

  private List<RID> create(final int count) {
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        rids.add(database.newDocument("Doc").set("i", i).save().getIdentity());
    });
    return rids;
  }

  private void delete(final RID rid) {
    database.transaction(() -> database.deleteRecord(rid.asDocument()));
  }

  /** The RID a new record of a fresh transaction on another thread would take, rolled back so the probe leaves nothing behind. */
  private RID probeFromAnotherThread() throws InterruptedException {
    final AtomicReference<RID> probe = new AtomicReference<>();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread thread = new Thread(() -> {
      try {
        database.begin();
        probe.set(database.newDocument("Doc").save().getIdentity());
        database.rollback();
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    thread.start();
    thread.join();
    assertThat(failure.get()).isNull();
    return probe.get();
  }

  @Test
  void aHeldHoleInsideTheRecordTableIsSkipped() throws InterruptedException {
    final List<RID> rids = create(3);
    final RID middle = rids.get(1);

    bucket().holdPosition(middle.getPosition());
    delete(middle);

    assertThat(probeFromAnotherThread()).isNotEqualTo(middle);

    bucket().releaseHeldPosition(middle.getPosition());
    assertThat(probeFromAnotherThread()).as("the released hole is reused").isEqualTo(middle);
  }

  @Test
  void aHeldSlotTheCommitTrimmedOffTheEndIsNotTheNextAppend() throws InterruptedException {
    final List<RID> rids = create(3);
    final RID last = rids.get(2);

    bucket().holdPosition(last.getPosition());
    delete(last);

    assertThat(probeFromAnotherThread()).isNotEqualTo(last);

    bucket().releaseHeldPosition(last.getPosition());
    assertThat(probeFromAnotherThread()).as("the released trailing slot is the next append again").isEqualTo(last);
  }

  @Test
  void aPageEmptiedWhileHeldIsNotAppendedToUntilReleased() throws InterruptedException {
    final List<RID> rids = create(1);
    final RID only = rids.getFirst();

    bucket().holdPosition(only.getPosition());
    delete(only);

    assertThat(probeFromAnotherThread()).isNotEqualTo(only);

    bucket().releaseHeldPosition(only.getPosition());
    assertThat(probeFromAnotherThread()).isEqualTo(only);
  }

  /**
   * An emptied page past page 0 is offered to a continuation chunk without the slot walk and without the insert reservation,
   * so the held first slot has to be refused there too.
   */
  @Test
  void aContinuationChunkIsNotOfferedTheHeldFirstSlotOfAnEmptiedPage() {
    final String big = "x".repeat(bucket().getPageSize() / 2);
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 2; i++)
        rids.add(database.newDocument("Doc").set("payload", big).save().getIdentity());
    });
    final int maxRecordsInPage = bucket().getMaxRecordsInPage();
    final RID onPage1 = rids.get(1);
    assertThat(onPage1.getPosition()).as("the second record opens page 1").isEqualTo(maxRecordsInPage);

    bucket().holdPosition(onPage1.getPosition());
    delete(onPage1);

    database.begin();
    try {
      assertThat(bucket().getSlotOfferedInPageForTesting(1, true)).as("the held slot is refused").isEqualTo(-1);
      assertThat(bucket().getSlotOfferedInPageForTesting(1, false)).isEqualTo(-1);
    } finally {
      database.rollback();
    }

    bucket().releaseHeldPosition(onPage1.getPosition());
    database.begin();
    try {
      assertThat(bucket().getSlotOfferedInPageForTesting(1, true)).as("the released slot is offered again").isEqualTo(0);
    } finally {
      database.rollback();
    }
  }

  @Test
  void anUnheldBucketAllocatesAsBefore() throws InterruptedException {
    final List<RID> rids = create(3);
    delete(rids.get(1));
    assertThat(probeFromAnotherThread()).isEqualTo(rids.get(1));
  }
}
