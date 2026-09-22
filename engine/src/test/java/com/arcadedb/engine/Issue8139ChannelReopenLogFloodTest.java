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
import com.arcadedb.log.WarningCapture;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8139
 * <p>
 * {@code WALFile.retryAfterReopen()} restores the interrupt flag after the retry, deliberately, so the next channel
 * operation on the reopened channel is closed and reopened again. It logged every reopen at {@code SEVERE}, so a
 * recovery scan on an interrupted thread emitted one {@code SEVERE} line per read. The first reopen is still
 * {@code SEVERE}; the rest of the burst goes to {@code FINE} and is counted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8139ChannelReopenLogFloodTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
  }

  @Test
  void anInterruptedRecoveryReadLogsOneSevereLineAndCountsEveryReopen() throws Exception {
    final File path = new File(database.getDatabasePath(), "txlog_8139_read.wal");
    final WALFile walFile = new WALFile(path.getAbsolutePath());
    try {
      final List<MutablePage> pages = Collections.emptyList();
      walFile.append(WALFile.writeTransactionToBuffer(pages, 4242L).getByteBuffer());

      final WALFile.WALTransaction[] tx = new WALFile.WALTransaction[1];
      final List<WarningCapture.LogLine> lines = WarningCapture.capture(Level.FINE, () -> {
        Thread.currentThread().interrupt();
        try {
          tx[0] = walFile.getFirstTransaction();
        } finally {
          Thread.interrupted();
        }
      });

      final List<WarningCapture.LogLine> reopenLines = lines.stream().filter(l -> l.message().contains("txlog_8139_read.wal"))
          .toList();

      assertThat(tx[0]).isNotNull();
      assertThat(tx[0].txId).isEqualTo(4242L);
      assertThat(walFile.getReopenCount()).as("the restored interrupt flag closes the channel again on every read")
          .isGreaterThan(1);
      assertThat(reopenLines.stream().filter(l -> l.level() == Level.SEVERE).count()).as("lines: %s", reopenLines)
          .isEqualTo(1);
      assertThat(reopenLines).as("every reopen is still traceable at FINE").hasSize((int) walFile.getReopenCount());
    } finally {
      walFile.close();
    }
  }

  @Test
  void severeAgainAfterAQuietIntervalWithTheFoldedCount() {
    final AtomicLong now = new AtomicLong(1_000L);
    final ChannelReopenLog log = new ChannelReopenLog(now::get);

    assertThat(log.record()).as("the first reopen is SEVERE").isZero();
    assertThat(log.record()).isEqualTo(-1);
    assertThat(log.record()).isEqualTo(-1);

    now.addAndGet(ChannelReopenLog.SEVERE_INTERVAL_NANOS - 1);
    assertThat(log.record()).as("still inside the interval").isEqualTo(-1);

    now.addAndGet(1);
    assertThat(log.record()).as("a later burst is SEVERE again and says how many were folded").isEqualTo(3);
    assertThat(log.record()).isEqualTo(-1);
    assertThat(log.getTotal()).isEqualTo(6);
  }
}
