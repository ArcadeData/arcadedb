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

import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11283: the "one commit" proofs of the multi-page record read redo their check under the publication lock when a
 * publication overlapped. On a replica that lock does not exclude the replicated apply, so a publication still open (or moved)
 * inside the lock voids the proof: a chain in motion is reported as changed, never accepted, and never condemned as broken.
 * The test holds a publication open with {@link PageManager#beginPublication()}, which is what the replicated apply does.
 */
class Issue11283ReplicaChainReadUnderApplyTest extends BucketPageLayoutTestSupport {

  @Test
  void aChainReadWhileAnApplyIsOpenIsNotAnsweredFromTheLock() {
    final RID[] rids = createChunkedRecords("Read");
    final LocalBucket bucket = bucketOf("Read");
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final byte[] expected = bucket.getRecordFromCommittedPage(rids[0]).toByteArray();

    pageManager.beginPublication();
    try {
      assertThatThrownBy(() -> bucket.getRecordFromCommittedPage(rids[0])).as("a chain in motion must not be accepted as one commit")
          .isInstanceOf(ConcurrentModificationException.class);
    } finally {
      pageManager.endPublication();
    }

    assertThat(bucket.getRecordFromCommittedPage(rids[0]).toByteArray()).as("settled again: read as before").isEqualTo(expected);
  }

  @Test
  void aPlaceholderReadWhileAnApplyIsOpenIsNotAnsweredFromTheLock() {
    database.transaction(() -> database.getSchema().createDocumentType("Placeholder", 1).createProperty("v", Type.STRING));
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Placeholder").set("v", "p").save().getIdentity());
    sealFirstPage("Placeholder");
    database.transaction(() -> rid[0].asDocument(true).modify().set("v", "b".repeat(20 * 1024)).save());
    assertThat((Long) bucketStats("Placeholder").get("totalPlaceholderRecords")).as("the fixture needs a placeholder").isEqualTo(1L);
    final LocalBucket bucket = bucketOf("Placeholder");
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final byte[] expected = bucket.getRecordFromCommittedPage(rid[0]).toByteArray();

    pageManager.beginPublication();
    try {
      assertThatThrownBy(() -> bucket.getRecordFromCommittedPage(rid[0])).isInstanceOf(ConcurrentModificationException.class);
    } finally {
      pageManager.endPublication();
    }

    assertThat(bucket.getRecordFromCommittedPage(rid[0]).toByteArray()).isEqualTo(expected);
  }

  @Test
  void aProofOfCorruptionIsVoidWhileAnApplyIsOpen() throws Exception {
    createChunkedRecords("Read");
    final LocalBucket bucket = bucketOf("Read");
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();

    assertThat(bucket.provenAgainstOneCommit(() -> "broken")).as("no publication: the proof stands").isEqualTo("broken");

    pageManager.beginPublication();
    try {
      assertThat(bucket.provenAgainstOneCommit(() -> "broken")).as("a publication open inside the lock voids the proof").isNull();
    } finally {
      pageManager.endPublication();
    }
  }

  @Test
  void aProofIsVoidWhenAPublicationMovesDuringIt() throws Exception {
    createChunkedRecords("Read");
    final LocalBucket bucket = bucketOf("Read");
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final AtomicInteger runs = new AtomicInteger();

    // The first run overlaps a publication (so the lock is taken), the second one, inside the lock, is overlapped again
    assertThat(bucket.provenAgainstOneCommit(() -> {
      pageManager.beginPublication();
      pageManager.endPublication();
      runs.incrementAndGet();
      return "broken";
    })).isNull();
    assertThat(runs.get()).isEqualTo(2);
  }
}
