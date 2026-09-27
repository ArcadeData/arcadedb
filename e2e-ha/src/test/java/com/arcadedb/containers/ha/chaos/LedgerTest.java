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

package com.arcadedb.containers.ha.chaos;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LedgerTest {

  @Test
  void keyRoundTrip() {
    final long key = Ledger.key(7, (1L << 39) + 5);
    assertThat(Ledger.writerOf(key)).isEqualTo(7);
    assertThat(Ledger.seqOf(key)).isEqualTo((1L << 39) + 5);
    assertThat(Ledger.format(Ledger.key(3, 17))).isEqualTo("w3-17");
  }

  @Test
  void reserveIsPerWriterAndStartsInFlight() {
    final Ledger ledger = new Ledger(2);
    assertThat(ledger.reserve(0, false)).isEqualTo(Ledger.key(0, 0));
    assertThat(ledger.reserve(0, true)).isEqualTo(Ledger.key(0, 1));
    assertThat(ledger.reserve(1, false)).isEqualTo(Ledger.key(1, 0));
    assertThat(ledger.size(0)).isEqualTo(2);
    assertThat(ledger.outcome(Ledger.key(0, 1))).isEqualTo(Ledger.IN_FLIGHT);
    assertThat(ledger.isPair(Ledger.key(0, 1))).isTrue();
    assertThat(ledger.isPair(Ledger.key(0, 0))).isFalse();
  }

  @Test
  void recordKeepsThePairFlag() {
    final Ledger ledger = new Ledger(1);
    final long key = ledger.reserve(0, true);
    ledger.record(key, Ledger.UNKNOWN);
    ledger.record(key, Ledger.ACKED_LATE);
    assertThat(ledger.outcome(key)).isEqualTo(Ledger.ACKED_LATE);
    assertThat(ledger.isPair(key)).isTrue();
    assertThat(ledger.count(Ledger.ACKED_LATE)).isEqualTo(1);
    assertThat(ledger.count(Ledger.UNKNOWN)).isZero();
  }

  @Test
  void unknownKeysAreRejected() {
    final Ledger ledger = new Ledger(1);
    assertThatThrownBy(() -> ledger.record(Ledger.key(0, 0), Ledger.ACKED)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ledger.outcome(Ledger.key(4, 0))).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void randomAckedKeyOnlyReturnsAcknowledgedKeysOfThatWriter() {
    final Ledger ledger = new Ledger(2);
    assertThat(ledger.randomAckedKey(0, new Random(1))).isEqualTo(-1);
    final long acked = ledger.reserve(0, false);
    ledger.record(acked, Ledger.ACKED);
    final long failed = ledger.reserve(0, false);
    ledger.record(failed, Ledger.FAILED);
    final long other = ledger.reserve(1, false);
    ledger.record(other, Ledger.ACKED);
    final Random random = new Random(3);
    for (int i = 0; i < 100; i++)
      assertThat(ledger.randomAckedKey(0, random)).isIn(acked, -1L);
  }

  @Test
  void growsBeyondTheInitialCapacityUnderConcurrentWriters() throws InterruptedException {
    final Ledger ledger = new Ledger(4);
    final List<Thread> threads = new ArrayList<>();
    for (int w = 0; w < 4; w++) {
      final int writer = w;
      threads.add(Thread.ofPlatform().start(() -> {
        for (int i = 0; i < 10_000; i++)
          ledger.record(ledger.reserve(writer, i % 5 == 0), Ledger.ACKED);
      }));
    }
    for (final Thread thread : threads)
      thread.join();
    assertThat(ledger.count(Ledger.ACKED)).isEqualTo(40_000);
    assertThat(ledger.count(Ledger.IN_FLIGHT)).isZero();
    assertThat(ledger.size(3)).isEqualTo(10_000);
  }
}
