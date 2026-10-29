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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #10875: {@code LocalBucket.freeChunkChain} zeroed the slot-table entry of the slot a chain
 * pointer reached BEFORE it checked that the slot held a continuation chunk of the chain. When the check failed the
 * slot was already freed, so a slot a concurrent insert had reused inside the window between the read of the chain
 * and the free of its tail lost its record - gone from {@code scan()}, {@code count()} and {@code lookupByRID}, in a
 * transaction that committed normally.
 * <p>
 * The concurrent window is not reproducible deterministically, so the walk is handed the state it would find there
 * directly: a chain pointer that reaches a slot holding an unrelated live record, and one that reaches a free slot.
 */
class Issue10875FreeChunkChainValidatesBeforeFreeingTest extends TestHelper {

  private static final String TYPE = "Issue10875";

  @Test
  void aPointerReachingALiveRecordLeavesThatRecordInPlace() throws Exception {
    database.getSchema().createDocumentType(TYPE);

    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newDocument(TYPE).set("tag", "SHRINKING").save().getIdentity();
      rids[1] = database.newDocument(TYPE).set("tag", "VICTIM").save().getIdentity();
    });
    final RID shrinking = rids[0];
    final RID victim = rids[1];
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(victim.getBucketId());

    // A RID position IS the encoding a next-chunk pointer uses: pageNumber * maxRecordsInPage + slot.
    database.transaction(() -> freeChunkChain(bucket, shrinking, victim.getPosition()));

    assertThat(database.lookupByRID(victim, true).asDocument().getString("tag")).isEqualTo("VICTIM");
    assertThat(database.countBucket(bucket.getName())).isEqualTo(2L);
    assertThat(database.query("sql", "select count() as c from " + TYPE).next().<Long>getProperty("c")).isEqualTo(2L);

    // The victim is still a plain record that can be updated and read back.
    database.transaction(() -> {
      final MutableDocument doc = database.lookupByRID(victim, true).asDocument().modify();
      doc.set("tag", "VICTIM-UPDATED").save();
    });
    assertThat(database.lookupByRID(victim, true).asDocument().getString("tag")).isEqualTo("VICTIM-UPDATED");
  }

  @Test
  void aPointerReachingAFreeSlotDoesNotFailTheTransaction() throws Exception {
    database.getSchema().createDocumentType(TYPE);

    final RID[] rids = new RID[3];
    database.transaction(() -> {
      rids[0] = database.newDocument(TYPE).set("tag", "SHRINKING").save().getIdentity();
      rids[1] = database.newDocument(TYPE).set("tag", "DELETED").save().getIdentity();
      rids[2] = database.newDocument(TYPE).set("tag", "NEIGHBOUR").save().getIdentity();
    });
    database.transaction(() -> database.lookupByRID(rids[1], true).delete());
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(rids[1].getBucketId());

    database.transaction(() -> freeChunkChain(bucket, rids[0], rids[1].getPosition()));

    assertThat(database.countBucket(bucket.getName())).isEqualTo(2L);
    assertThat(database.lookupByRID(rids[0], true).asDocument().getString("tag")).isEqualTo("SHRINKING");
    assertThat(database.lookupByRID(rids[2], true).asDocument().getString("tag")).isEqualTo("NEIGHBOUR");
  }

  private static void freeChunkChain(final LocalBucket bucket, final RID originalRID, final long chunkPointer) {
    try {
      final Method method = LocalBucket.class.getDeclaredMethod("freeChunkChain", RID.class, long.class);
      method.setAccessible(true);
      method.invoke(bucket, originalRID, chunkPointer);
    } catch (final ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
