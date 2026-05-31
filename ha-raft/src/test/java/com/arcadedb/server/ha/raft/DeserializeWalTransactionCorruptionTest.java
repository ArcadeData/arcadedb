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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4420: a malformed WAL transaction entry (e.g. produced by a leader running a version
 * affected by the backward-shift WAL-range bug #4319) carried a page delta whose {@code changesTo < changesFrom}.
 * Decoding it on a follower computed a negative {@code deltaSize} and blew up with a cryptic
 * {@code NegativeArraySizeException("-51")} on {@code new byte[deltaSize]}, which failed the leader's quorum and surfaced
 * to the client as an opaque HTTP 500. The decoder must now reject such an entry with a clear {@link ReplicationException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DeserializeWalTransactionCorruptionTest {

  @Test
  void negativeDeltaSizeIsRejectedClearly() {
    // changesFrom=100, changesTo=48 -> deltaSize = 48 - 100 + 1 = -51 (the exact value reported in issue #4420)
    final byte[] entry = buildSinglePageEntry(100, 48);

    assertThatThrownBy(() -> ArcadeStateMachine.deserializeWalTransaction(entry))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("invalid delta range");
  }

  @Test
  void oversizedDeltaSizeIsRejectedClearly() {
    // A delta claiming far more bytes than the entry actually contains is corruption too.
    final byte[] entry = buildSinglePageEntry(0, 1_000_000);

    assertThatThrownBy(() -> ArcadeStateMachine.deserializeWalTransaction(entry))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("invalid delta range");
  }

  @Test
  void negativePageCountIsRejectedClearly() {
    final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES);
    buf.putLong(1L);  // txId
    buf.putLong(0L);  // timestamp
    buf.putInt(-7);   // pageCount (corrupted)
    buf.putInt(0);    // segmentSize

    assertThatThrownBy(() -> ArcadeStateMachine.deserializeWalTransaction(buf.array()))
        .isInstanceOf(ReplicationException.class)
        .hasMessageContaining("invalid page count");
  }

  /**
   * Builds a one-page WAL transaction entry in the format read by {@code ArcadeStateMachine.deserializeWalTransaction}:
   * txId(long), timestamp(long), pageCount(int), segmentSize(int), then per page fileId, pageNumber, changesFrom,
   * changesTo, version, contentSize (6 ints). No delta bytes are appended (the corrupted range is rejected first).
   */
  private static byte[] buildSinglePageEntry(final int changesFrom, final int changesTo) {
    final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES + 6 * Integer.BYTES);
    buf.putLong(1L);  // txId
    buf.putLong(0L);  // timestamp
    buf.putInt(1);    // pageCount
    buf.putInt(0);    // segmentSize (unused by the decoder)
    buf.putInt(0);    // fileId
    buf.putInt(0);    // pageNumber
    buf.putInt(changesFrom);
    buf.putInt(changesTo);
    buf.putInt(1);    // version
    buf.putInt(0);    // contentSize
    return buf.array();
  }
}
