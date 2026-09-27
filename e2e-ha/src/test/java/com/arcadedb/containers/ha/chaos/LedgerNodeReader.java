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

/**
 * A perfect cluster: every node holds exactly the acknowledged writes, except {@code dropKey}, which every node lost.
 */
final class LedgerNodeReader implements NodeReader {
  private final Ledger ledger;
  long dropKey = -1;

  LedgerNodeReader(final Ledger ledger) {
    this.ledger = ledger;
  }

  @Override
  public long[] counts(final int node) {
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    scan(node, snapshot);
    long edges = 0;
    for (int w = 0; w < ledger.writers(); w++)
      for (int s = 0; s < ledger.size(w); s++)
        if (snapshot.hasEdge(w, s))
          ++edges;
    return new long[] { snapshot.rows(), edges };
  }

  @Override
  public void scan(final int node, final NodeSnapshot sink) {
    for (int w = 0; w < ledger.writers(); w++)
      for (int s = 0; s < ledger.size(w); s++) {
        final long key = Ledger.key(w, s);
        final byte outcome = ledger.outcome(key);
        if ((outcome == Ledger.ACKED || outcome == Ledger.ACKED_LATE) && key != dropKey)
          sink.add(key, ledger.isPair(key) ? 1 : 0);
      }
  }

  @Override
  public void scanRecords(final int node, final RecordScan sink) {
    final NodeSnapshot snapshot = new NodeSnapshot(ledger);
    scan(node, snapshot);
    final long[] position = { 0 };
    snapshot.forEachKey(key -> sink.add(RecordScan.rid(1, position[0]++), key));
  }
}
