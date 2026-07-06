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
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the {@link LSMTreeIndex#close()} finding in issue #4960: close() only CASed
 * {@code AVAILABLE -> UNAVAILABLE}, so when a compaction was merely SCHEDULED (not yet running) the CAS
 * failed and close() silently did nothing - the index files stayed open and the compactor could still
 * kick in against a closing database. A scheduled-but-not-started compaction is now cancelled and the
 * index closed; an actually running compaction is logged loudly instead of being ignored in silence.
 */
class Issue4960LSMCloseDuringCompactionTest extends TestHelper {

  @Test
  void closeCancelsScheduledCompaction() throws Exception {
    database.transaction(() -> {
      database.getSchema().createDocumentType("CloseIdx").createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "CloseIdx", "id");
      for (int i = 0; i < 10; i++)
        database.newDocument("CloseIdx").set("id", i).save();
    });

    final LSMTreeIndex lsm = Arrays.stream(database.getSchema().getIndexes())
        .filter(LSMTreeIndex.class::isInstance)
        .map(LSMTreeIndex.class::cast)
        .findFirst()
        .orElseThrow();

    assertThat(lsm.scheduleCompaction()).isTrue();

    lsm.close();

    assertThat(lsm.status.get())
        .as("close() must cancel a scheduled-but-not-started compaction and mark the index unavailable")
        .isEqualTo(IndexInternal.INDEX_STATUS.UNAVAILABLE);

    // A cancelled schedule must not be compactable afterwards.
    assertThat(lsm.compact()).isFalse();

    reopenDatabase();
  }
}
