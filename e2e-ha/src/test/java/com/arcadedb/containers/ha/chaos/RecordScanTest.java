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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordScanTest {
  private final Ledger ledger = new Ledger(1);
  private final long   a      = acked();
  private final long   b      = acked();

  private long acked() {
    final long key = ledger.reserve(0, false);
    ledger.record(key, Ledger.ACKED);
    return key;
  }

  private NodeSnapshot index(final long... keys) {
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    for (final long key : keys)
      snapshot.add(key, 0);
    return snapshot;
  }

  @Test
  void bucketsMatchingTheIndexReportNothing() {
    final RecordScan records = new RecordScan();
    records.add(RecordScan.rid(1, 0), a);
    records.add(RecordScan.rid(2, 0), b);
    assertThat(records.anomalies(0, index(a, b), 10)).isEmpty();
  }

  @Test
  void secondRecordWithAnExistingIdIsADuplicate() {
    final RecordScan records = new RecordScan();
    records.add(RecordScan.rid(1, 0), a);
    records.add(RecordScan.rid(1, 1), b);
    records.add(RecordScan.rid(3, 4), b);
    assertThat(records.anomalies(1, index(a, b), 10)).containsExactly(
        "node 1 records: 3 records, 2 distinct ids, 2 index entries",
        "node 1 records: " + Ledger.format(b) + " is held by 2 records [#1:1, #3:4] (duplicate id)");
  }

  @Test
  void recordWithoutIndexEntryAndIndexEntryWithoutRecord() {
    final RecordScan records = new RecordScan();
    records.add(RecordScan.rid(1, 0), a);
    records.add(RecordScan.rid(2, 5), RecordScan.NO_ID);
    assertThat(records.anomalies(2, index(b), 10)).containsExactly(
        "node 2 records: 2 records, 1 distinct ids, 1 index entries",
        "node 2 records: #2:5 has no id",
        "node 2 records: " + Ledger.format(a) + " at #1:0 is not in the index",
        "node 2 records: index entry " + Ledger.format(b) + " has no record");
  }

  @Test
  void linesAreCappedAtTheLimit() {
    final RecordScan records = new RecordScan();
    for (int i = 0; i < 5; i++)
      records.add(RecordScan.rid(1, i), RecordScan.NO_ID);
    assertThat(records.anomalies(0, index(), 2)).hasSize(3);
  }

  @Test
  void ridRoundTrips() {
    assertThat(RecordScan.formatRid(RecordScan.parseRid("#17:123456789"))).isEqualTo("#17:123456789");
    assertThatThrownBy(() -> RecordScan.parseRid("17:1")).isInstanceOf(IllegalArgumentException.class);
  }
}
