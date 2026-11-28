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
package com.arcadedb.bench.workload.ha;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WriteLedgerTest {
  @Test
  void ackedIdMissingOnANodeIsReported() {
    final WriteLedger ledger = new WriteLedger(4);
    ledger.acked(7);
    ledger.acked(3);
    ledger.acked(5);
    assertThat(ledger.ackedCount()).isEqualTo(3);
    assertThat(ledger.missingFrom(new long[] { 3, 4, 7 })).containsExactly(5);
    assertThat(ledger.missingFrom(new long[] { 3, 5, 7 })).isEmpty();
    assertThat(ledger.missingFrom(new long[0])).containsExactly(3, 5, 7);
  }

  @Test
  void unknownIdsAreNeverMissing() {
    final WriteLedger ledger = new WriteLedger(4);
    ledger.acked(1);
    ledger.unknown(2);
    ledger.unknown(3);
    assertThat(ledger.unknownCount()).isEqualTo(2);
    assertThat(ledger.ackedCount()).isEqualTo(1);
    // an unknown write may have landed or not: neither is a miss
    assertThat(ledger.missingFrom(new long[] { 1 })).isEmpty();
    assertThat(ledger.missingFrom(new long[] { 1, 2, 3 })).isEmpty();
  }

  @Test
  void growsPastItsHint() {
    final WriteLedger ledger = new WriteLedger(1);
    for (long id = 0; id < 10_000; id++)
      if (id % 10 == 0)
        ledger.unknown(id);
      else
        ledger.acked(id);
    assertThat(ledger.ackedCount()).isEqualTo(9_000);
    assertThat(ledger.unknownCount()).isEqualTo(1_000);
    final long[] onNode = new long[8_999];
    int n = 0;
    for (long id = 0; id < 10_000; id++)
      if (id % 10 != 0 && id != 9_999)
        onNode[n++] = id;
    assertThat(ledger.missingFrom(onNode)).containsExactly(9_999);
  }

  @Test
  void anotherLedgerIsMergedIn() {
    final WriteLedger a = new WriteLedger(2);
    a.acked(1);
    a.unknown(2);
    final WriteLedger b = new WriteLedger(2);
    b.acked(3);
    b.unknown(4);
    a.addAll(b);
    assertThat(a.ackedCount()).isEqualTo(2);
    assertThat(a.unknownCount()).isEqualTo(2);
    assertThat(a.missingFrom(new long[] { 1 })).containsExactly(3);
  }
}
