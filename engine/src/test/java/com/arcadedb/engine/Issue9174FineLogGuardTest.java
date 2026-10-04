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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.spy;

/**
 * Issue #9174: the record create, update, and delete paths of {@link LocalBucket}, the page compaction a commit runs
 * on every bucket page it modified, and the per-page loop of {@link WALFile#writeTransactionToBuffer} called
 * {@code LogManager.log(..., Level.FINE, ...)} with no level check. Each call boxes its arguments and looks the
 * requester's logger up before the logger finds FINE disabled, and {@code defragPage} makes one per slot it moves, so a
 * delete that leaves records behind it in its page paid for it once per moved record. These calls are now made only
 * when {@link LogManager#isDebugEnabled()}, as the FINE calls of {@code LSMTreeIndexMutable} and
 * {@code TransactionManager} already are.
 * <p>
 * The same workload runs with debug off and with debug on. With debug off it must not request any FINE message from
 * the bucket or from the WAL buffer. With debug on it must request every guarded message it reaches, which is what
 * proves the workload walks those paths and the first assertion is not an empty capture.
 */
class Issue9174FineLogGuardTest extends TestHelper {
  private static final String TYPE    = "FineLog9174";
  private static final int    RECORDS = 12;

  /**
   * The guarded calls the workload reaches: ten of the eleven in {@link LocalBucket} (not the placeholder spill of an
   * update, which needs a slot too small for a chunk header) and the per-page one of the WAL buffer.
   */
  private static final Set<String> GUARDED_AND_REACHED = Set.of(
      "Creating record (%s records=%d threadId=%d)",
      "Created record %s (%s records=%d threadId=%d)",
      "Updated record %s with the same size or less as before (%s threadId=%d)",
      "Updated record %s by allocating new space on the same page (%s threadId=%d)",
      "Deleted record %s (%s threadId=%d)",
      "Moving segment page %s %d-(%d)->%d...",
      "- record %d %d->%d",
      "Compressed page %s removed %d holes",
      "Update record count from %d to %d in page %s",
      "Update record count from %d to 0 in page %s",
      "Writing page %s v%d range %d-%d into buffer (txId=%d threadId=%d)");

  @Override
  protected void beginTest() {
    database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(1).create();
  }

  @Test
  void withDebugOffTheRecordAndCompactionPathsRequestNoFineMessage() {
    assertThat(fineMessagesRequestedBy(false))
        .as("with debug off, writing records and compacting their page must not call the logger at FINE")
        .isEmpty();
  }

  @Test
  void withDebugOnTheSameWorkloadRequestsEveryGuardedMessage() {
    assertThat(fineMessagesRequestedBy(true))
        .as("the workload must reach every guarded call, or the debug-off assertion proves nothing")
        .containsAll(GUARDED_AND_REACHED);
  }

  /** Runs the workload with the debug flag set as given; returns the FINE messages the bucket and the WAL buffer asked for. */
  private Set<String> fineMessagesRequestedBy(final boolean debug) {
    final LogManager logManager = LogManager.instance();
    final boolean previousDebug = logManager.isDebugEnabled();
    final Logger previous = logManager.getLogger();
    final Logger capture = spy(previous);
    logManager.setDebugEnabled(debug);
    logManager.setLogger(capture);
    try {
      runWorkload();
    } finally {
      logManager.setLogger(previous);
      logManager.setDebugEnabled(previousDebug);
    }

    return mockingDetails(capture).getInvocations().stream()
        .filter(invocation -> invocation.getMethod().getName().equals("log"))
        .map(invocation -> invocation.getArguments())
        .filter(arguments -> arguments[1] == Level.FINE && (arguments[0] instanceof LocalBucket || arguments[0] == WALFile.class))
        .map(arguments -> (String) arguments[2])
        .collect(Collectors.toSet());
  }

  /** One page of one bucket: inserts, a shrink, a growth, then deletes that leave holes until the page is empty. */
  private void runWorkload() {
    final RID[] rids = new RID[RECORDS];
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        rids[i] = database.newDocument(TYPE).set("v", value(i)).save().getIdentity();
    });

    // A SHRINK IS WRITTEN IN PLACE, AND THE COMMIT CLOSES THE HOLE IT LEAVES
    database.transaction(() -> rids[4].asDocument(true).modify().set("v", "short").save());

    // A GROWTH THAT STILL FITS IN THE PAGE SHIFTS THE RECORDS BEHIND IT
    database.transaction(() -> rids[5].asDocument(true).modify().set("v", value(5) + "y".repeat(100)).save());

    // A DELETE WITH LIVE RECORDS BEHIND IT: THE COMMIT MOVES THEM AND REWRITES THEIR SLOTS
    database.transaction(() -> rids[0].asDocument(true).delete());

    // THE LAST RECORD AND ONE IN THE MIDDLE: THE COMMIT ALSO LOWERS THE RECORD COUNT OF THE PAGE
    database.transaction(() -> {
      rids[RECORDS - 1].asDocument(true).delete();
      rids[1].asDocument(true).delete();
    });

    // EVERY RECORD LEFT: THE COMMIT RESETS THE RECORD COUNT OF THE NOW EMPTY PAGE
    database.transaction(() -> {
      for (int i = 2; i < RECORDS - 1; i++)
        rids[i].asDocument(true).delete();
    });
  }

  private static String value(final int i) {
    return "record-" + i + "-" + "x".repeat(200);
  }
}
