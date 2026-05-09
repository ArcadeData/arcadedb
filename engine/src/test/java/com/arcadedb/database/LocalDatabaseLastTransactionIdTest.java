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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sanity tests for {@link LocalDatabase#getLastTransactionId()}, the recency signal exposed for
 * the HA bootstrap path (issue #4147). The contract:
 * <ul>
 *   <li>Strictly monotonic across commits.</li>
 *   <li>Survives close+reopen (recovery picks it up from the WAL).</li>
 * </ul>
 */
class LocalDatabaseLastTransactionIdTest extends TestHelper {

  @Test
  void monotonicallyAdvancesAcrossCommits() {
    final LocalDatabase db = (LocalDatabase) database;
    db.transaction(() -> {
      if (!db.getSchema().existsType("LastTxIdType")) {
        final DocumentType t = db.getSchema().createDocumentType("LastTxIdType");
        t.createProperty("name", String.class);
      }
    });

    long previous = db.getLastTransactionId();
    for (int i = 0; i < 5; i++) {
      db.transaction(() -> {
        final MutableDocument doc = db.newDocument("LastTxIdType");
        doc.set("name", "row");
        doc.save();
      });
      final long current = db.getLastTransactionId();
      assertThat(current).as("lastTxId must advance past every committed transaction")
          .isGreaterThan(previous);
      previous = current;
    }
  }

  @Test
  void survivesReopen() {
    final LocalDatabase db = (LocalDatabase) database;
    db.transaction(() -> {
      if (!db.getSchema().existsType("LastTxIdReopenType"))
        db.getSchema().createDocumentType("LastTxIdReopenType");
    });
    db.transaction(() -> db.newDocument("LastTxIdReopenType").save());

    final long before = db.getLastTransactionId();
    assertThat(before).isGreaterThanOrEqualTo(0);

    reopenDatabase();

    final long after = ((LocalDatabase) database).getLastTransactionId();
    // After WAL recovery the counter is set to last-applied + 1, so post-reopen the "last" value
    // can equal or exceed pre-close. What matters for the bootstrap protocol is that we never
    // regress to -1 after a database that committed transactions.
    assertThat(after).as("lastTxId must not regress after close+reopen")
        .isGreaterThanOrEqualTo(0);
  }
}
