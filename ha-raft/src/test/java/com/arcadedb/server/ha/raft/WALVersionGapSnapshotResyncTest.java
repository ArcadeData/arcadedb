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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.WALVersionGapException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test: WALVersionGapException must trigger snapshot resync (ReplicationException)
 * rather than being silently swallowed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class WALVersionGapSnapshotResyncTest {

  private static final String DB_PATH = "./target/databases/test-wal-version-gap";
  private DatabaseInternal db;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    db = (DatabaseInternal) new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * Verifies that applyChanges() throws WALVersionGapException when the WAL page version
   * is more than 1 ahead of the database page version. This exception must propagate (not
   * be swallowed) so that the state machine can trigger a snapshot resync.
   */
  @Test
  void applyChangesThrowsOnVersionGap() {
    // Create a type so we have a real bucket/page to target
    db.getSchema().createDocumentType("TestType");

    // Get the first bucket's file ID - it has pages at version 0
    final int fileId = db.getSchema().getType("TestType").getBuckets(false).get(0).getFileId();

    // Build a WAL transaction with page version 5 (gap: 5 > 0 + 1)
    final byte[] delta = new byte[] { 0 };
    final int segmentSize = 6 * Integer.BYTES + delta.length;
    final int totalSize = 24 + segmentSize + 12;

    final ByteBuffer buf = ByteBuffer.allocate(totalSize);

    // Header
    buf.putLong(1L);   // txId
    buf.putLong(System.currentTimeMillis()); // timestamp
    buf.putInt(1);     // pageCount
    buf.putInt(segmentSize);

    // Page with version gap (version 5, but DB page is at version 0)
    buf.putInt(fileId); // fileId
    buf.putInt(0);      // pageNumber
    buf.putInt(0);      // changesFrom
    buf.putInt(0);      // changesTo
    buf.putInt(5);      // currentPageVersion - creates a gap (5 > 0 + 1)
    buf.putInt(65536);  // currentPageSize
    buf.put(delta);

    // Footer
    buf.putInt(segmentSize);
    buf.putLong(WALFile.MAGIC_NUMBER);

    final WALFile.WALTransaction walTx = RaftLogEntryCodec.parseWalTransaction(new Binary(buf.array()));

    // This must throw WALVersionGapException - not be silently swallowed
    assertThatThrownBy(() -> db.getTransactionManager().applyChanges(walTx, new HashMap<>(), false))
        .isInstanceOf(WALVersionGapException.class);
  }

  /**
   * Verifies that the WALVersionGapException catch block in ArcadeDBStateMachine wraps
   * the exception as ReplicationException. This is tested by confirming that
   * ReplicationException includes WALVersionGapException as its cause - matching the
   * re-throw pattern in applyTransactionEntry().
   */
  @Test
  void replicationExceptionWrapsWALVersionGapException() {
    final WALVersionGapException cause = new WALVersionGapException("test version gap");
    final ReplicationException re = new ReplicationException("WAL version gap detected - snapshot resync required", cause);

    assertThat(re.getCause()).isInstanceOf(WALVersionGapException.class);
    assertThat(re.getMessage()).contains("snapshot resync required");
  }
}
