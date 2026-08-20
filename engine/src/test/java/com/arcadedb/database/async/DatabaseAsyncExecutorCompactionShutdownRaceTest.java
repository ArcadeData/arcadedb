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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Discord report (2026-08-14/19, "heimdall" database, #6505): production log analysis traced a database
 * fencing incident to this exact race. {@code DatabaseAsyncExecutorImpl.getBestSlot()}/{@code scheduleTask}
 * throw {@code DatabaseOperationException("Async executor has been shut down")} both for a genuine terminal
 * {@code close()}/{@code kill()} (#4955, {@link AsyncSlotShutdownRaceTest}) AND for the momentary window
 * {@code setTransactionUseWAL()}/{@code setTransactionSync()} leaves while {@code createThreads()} tears down
 * and respawns the whole pool for an UNRELATED caller (#5665 - GraphBatch toggling durability on open/close).
 * <p>
 * {@code LSMTreeIndexMutable.onAfterCommit()} calls {@code compact(index)} AFTER the committing transaction's
 * WAL record was already written - past the point of no return (#5053) - so letting that exception escape
 * fenced the entire database over a best-effort background compaction-scheduling hiccup that had nothing to
 * do with the committing transaction's own data. {@code compact()} must contain it exactly like the
 * documented "full queue" case: no compaction lost, picked up by the next commit past the threshold.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DatabaseAsyncExecutorCompactionShutdownRaceTest extends TestHelper {

  @Test
  void compactionSchedulingFailureDuringShutdownDoesNotEscapeOrLeaveTheIndexStuck() {
    final DatabaseInternal db = (DatabaseInternal) database;

    final Index index = db.getSchema().createDocumentType("Doc").createProperty("v", Type.INTEGER)
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
    // The per-bucket index, not the TypeIndex aggregate: TypeIndex.setStatus() is a no-op stub (it has no
    // single status of its own), so compact()'s reservation/release only has an observable effect on the
    // concrete underlying index - the same one LSMTreeIndexMutable.onAfterCommit() actually calls compact() on.
    final IndexInternal indexInternal = ((TypeIndex) index).getIndexesOnBuckets()[0];

    final DatabaseAsyncExecutorImpl async = (DatabaseAsyncExecutorImpl) db.async();
    // Force the same executorThreads==null state a concurrent createThreads() teardown leaves momentarily
    // (#5665) - kill() is the deterministic test-only way to reach it, same technique AsyncSlotShutdownRaceTest
    // uses for the #4955 case this shares its exception with.
    async.kill();

    assertThatCode(() -> async.compact(indexInternal))
        .as("a background compaction that cannot be scheduled because the executor is (transiently or not) "
            + "unavailable must never escape and fence the database (#6505)")
        .doesNotThrowAnyException();

    // The reservation compact() takes via scheduleCompaction() must be handed back on this failure path too,
    // exactly like the documented full-queue case - otherwise the index silently stops compacting forever.
    // scheduleCompaction() re-succeeding is the public-API proof the status is back at AVAILABLE.
    assertThat(indexInternal.scheduleCompaction())
        .as("the index's AVAILABLE -> COMPACTION_SCHEDULED reservation must be released, not left stuck")
        .isTrue();
  }
}
