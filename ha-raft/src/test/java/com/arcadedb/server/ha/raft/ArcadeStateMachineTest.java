/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.engine.WALFile;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class ArcadeStateMachineTest {

  @Test
  void stateMachineCanBeInstantiated() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    assertThat(sm).isNotNull();
  }

  @Test
  void getLastAppliedTermIndexInitiallyNull() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    assertThat(sm.getLastAppliedTermIndex()).isNull();
  }

  @Test
  void deserializeWalTransactionRoundTrip() {
    // Build a minimal WAL transaction buffer manually
    final int pageCount = 2;
    final byte[] delta1 = new byte[] { 10, 20, 30 };
    final byte[] delta2 = new byte[] { 40, 50 };

    // Calculate segment size: sum of per-page header (6 ints = 24 bytes) + delta bytes
    final int segmentSize = (24 + delta1.length) + (24 + delta2.length);

    // Header: txId (8) + timestamp (8) + pageCount (4) + segmentSize (4) = 24
    // Pages: segmentSize bytes
    // Footer: segmentSize (4) + MAGIC_NUMBER (8) = 12
    final int totalSize = 24 + segmentSize + 12;

    final ByteBuffer buf = ByteBuffer.allocate(totalSize);

    // Header
    buf.putLong(42L);         // txId
    buf.putLong(1000L);       // timestamp
    buf.putInt(pageCount);    // page count
    buf.putInt(segmentSize);  // segment size

    // Page 1
    buf.putInt(1);     // fileId
    buf.putInt(0);     // pageNumber
    buf.putInt(100);   // changesFrom
    buf.putInt(102);   // changesTo (delta = 3 bytes)
    buf.putInt(5);     // currentPageVersion
    buf.putInt(4096);  // currentPageSize
    buf.put(delta1);

    // Page 2
    buf.putInt(2);     // fileId
    buf.putInt(3);     // pageNumber
    buf.putInt(200);   // changesFrom
    buf.putInt(201);   // changesTo (delta = 2 bytes)
    buf.putInt(8);     // currentPageVersion
    buf.putInt(8192);  // currentPageSize
    buf.put(delta2);

    // Footer
    buf.putInt(segmentSize);
    buf.putLong(WALFile.MAGIC_NUMBER);

    final WALFile.WALTransaction tx = ArcadeStateMachine.deserializeWalTransaction(buf.array());

    assertThat(tx.txId).isEqualTo(42L);
    assertThat(tx.timestamp).isEqualTo(1000L);
    assertThat(tx.pages).hasSize(2);

    // Verify page 1
    assertThat(tx.pages[0].fileId).isEqualTo(1);
    assertThat(tx.pages[0].pageNumber).isEqualTo(0);
    assertThat(tx.pages[0].changesFrom).isEqualTo(100);
    assertThat(tx.pages[0].changesTo).isEqualTo(102);
    assertThat(tx.pages[0].currentPageVersion).isEqualTo(5);
    assertThat(tx.pages[0].currentPageSize).isEqualTo(4096);
    assertThat(tx.pages[0].currentContent.size()).isEqualTo(3);

    // Verify page 2
    assertThat(tx.pages[1].fileId).isEqualTo(2);
    assertThat(tx.pages[1].pageNumber).isEqualTo(3);
    assertThat(tx.pages[1].changesFrom).isEqualTo(200);
    assertThat(tx.pages[1].changesTo).isEqualTo(201);
    assertThat(tx.pages[1].currentPageVersion).isEqualTo(8);
    assertThat(tx.pages[1].currentPageSize).isEqualTo(8192);
    assertThat(tx.pages[1].currentContent.size()).isEqualTo(2);
  }

  @Test
  void deserializeWalTransactionSinglePage() {
    final byte[] delta = new byte[] { 1, 2, 3, 4, 5 };
    final int segmentSize = 24 + delta.length;
    final int totalSize = 24 + segmentSize + 12;

    final ByteBuffer buf = ByteBuffer.allocate(totalSize);

    buf.putLong(99L);
    buf.putLong(2000L);
    buf.putInt(1);
    buf.putInt(segmentSize);

    buf.putInt(7);     // fileId
    buf.putInt(42);    // pageNumber
    buf.putInt(0);     // changesFrom
    buf.putInt(4);     // changesTo (delta = 5 bytes)
    buf.putInt(1);     // currentPageVersion
    buf.putInt(8192);  // currentPageSize
    buf.put(delta);

    buf.putInt(segmentSize);
    buf.putLong(WALFile.MAGIC_NUMBER);

    final WALFile.WALTransaction tx = ArcadeStateMachine.deserializeWalTransaction(buf.array());

    assertThat(tx.txId).isEqualTo(99L);
    assertThat(tx.timestamp).isEqualTo(2000L);
    assertThat(tx.pages).hasSize(1);
    assertThat(tx.pages[0].fileId).isEqualTo(7);
    assertThat(tx.pages[0].pageNumber).isEqualTo(42);
    assertThat(tx.pages[0].changesFrom).isEqualTo(0);
    assertThat(tx.pages[0].changesTo).isEqualTo(4);
    assertThat(tx.pages[0].currentContent.size()).isEqualTo(5);
  }

  @Test
  void notifyLeaderChangedDoesNotThrowWithoutRaftHAServer() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(
        RaftPeerId.valueOf("peer-0"), RaftGroupId.valueOf(UUID.randomUUID()));

    // Should not throw even without RaftHAServer set
    assertThatNoException().isThrownBy(() -> sm.notifyLeaderChanged(memberId, RaftPeerId.valueOf("peer-1")));
  }

  @Test
  void notifyLeaderChangedDoesNotThrowWithNullLeader() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final RaftGroupMemberId memberId = RaftGroupMemberId.valueOf(
        RaftPeerId.valueOf("peer-0"), RaftGroupId.valueOf(UUID.randomUUID()));

    assertThatNoException().isThrownBy(() -> sm.notifyLeaderChanged(memberId, null));
  }

  @Test
  void deserializeWalTransactionZeroPages() {
    final int segmentSize = 0;
    final int totalSize = 24 + 12; // header + footer only

    final ByteBuffer buf = ByteBuffer.allocate(totalSize);

    buf.putLong(1L);
    buf.putLong(500L);
    buf.putInt(0);
    buf.putInt(segmentSize);

    buf.putInt(segmentSize);
    buf.putLong(WALFile.MAGIC_NUMBER);

    final WALFile.WALTransaction tx = ArcadeStateMachine.deserializeWalTransaction(buf.array());

    assertThat(tx.txId).isEqualTo(1L);
    assertThat(tx.timestamp).isEqualTo(500L);
    assertThat(tx.pages).isEmpty();
  }
}
