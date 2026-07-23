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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for the explicit-lock file-collection gap surfaced by the single-server load test.
 * <p>
 * {@code LOCK TYPE <type>} must lock EVERY component file of every index of the type, not just the mutable
 * component. Once an LSM index has been compacted it owns two files (a mutable and a compacted sub-index),
 * both reported by {@link com.arcadedb.index.IndexInternal#getFileIds()}. The implicit-lock path
 * ({@code TransactionIndexContext.addFilesToLock}) adds ALL of them to the set of files a transaction must
 * hold locked at commit. If the explicit-lock collection only locks the mutable file
 * ({@code getFileId()}), the commit-time coverage check rejects every explicit-lock transaction on that
 * type with a non-retryable "not all the modified resources were locked" error, which {@code COMMIT RETRY}
 * cannot absorb.
 */
class ExplicitLockCompactedSubIndexTest extends TestHelper {

  private static final int PAGE_SIZE = 2 * 1024;

  @Override
  protected void beginTest() {
    // Disable automatic compaction scheduling so this test controls compaction deterministically; otherwise a
    // background auto-compaction leaves the index status != AVAILABLE and manual scheduleCompaction() no-ops.
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);
  }

  @Test
  void lockTypeCoversCompactedSubIndexFile() throws Exception {
    database.transaction(() -> {
      final var type = database.getSchema().buildDocumentType().withName("Photo").withTotalBuckets(1).create();
      type.createProperty("id", Integer.class);
      database.getSchema().buildTypeIndex("Photo", new String[] { "id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true)
          .withPageSize(PAGE_SIZE)
          .create();
    });

    // Populate so the mutable index has >= 2 pages (compaction requires >= 2 pages).
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        database.newDocument("Photo").set("id", i).save();
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Photo[id]");
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];

    assertThat(bucketIndex.getMutableIndex().getTotalPages()).isGreaterThanOrEqualTo(2);

    // Force a compaction so the index owns a compacted sub-index file in addition to the mutable one.
    assertThat(bucketIndex.scheduleCompaction()).isTrue();
    assertThat(bucketIndex.compact()).isTrue();

    // After compaction the index reports two component files: the mutable and the compacted sub-index.
    assertThat(bucketIndex.getFileIds()).hasSize(2);

    // An explicit LOCK TYPE followed by a write must commit cleanly: the coverage check requires the
    // compacted sub-index file to be locked too, and before the fix LOCK TYPE only locked the mutable one.
    assertThatCode(() -> {
      database.begin();
      database.acquireLock().type("Photo").lock();
      database.newDocument("Photo").set("id", 1_000_000).save();
      database.commit();
    }).doesNotThrowAnyException();

    assertThat(database.countType("Photo", true)).isEqualTo(2_001);
  }
}
