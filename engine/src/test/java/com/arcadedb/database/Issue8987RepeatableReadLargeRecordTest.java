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
package com.arcadedb.database;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8987: under {@code REPEATABLE_READ} a second read of a multi-page record must return the snapshot the
 * transaction already pinned, exactly as it does for a single-page record. It used to return the other transaction's
 * newer head chunk (when only the head moved) or fail with a {@code ConcurrentModificationException} (when a
 * continuation chunk moved too), because the chain validation compared the pinned pages with the newest committed ones.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8987RepeatableReadLargeRecordTest extends BucketPageLayoutTestSupport {
  @Test
  void largeRecordHeadChangedByAnotherTransaction() {
    assertRepeatable(70_000, false);
  }

  @Test
  void largeRecordHeadAndTailChangedByAnotherTransaction() {
    assertRepeatable(70_000, true);
  }

  @Test
  void smallRecordIsRepeatable() {
    assertRepeatable(1_000, true);
  }

  private void assertRepeatable(final int size, final boolean changeTail) {
    database.transaction(() -> database.getSchema().createDocumentType("Doc"));
    final RID[] rid = new RID[1];
    database.transaction(
        () -> rid[0] = database.newDocument("Doc").set("v", 0).set("s", "x".repeat(size)).save().getIdentity());

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      final Document first = database.lookupByRID(rid[0], true).asDocument();
      assertThat(first.getInteger("v")).isEqualTo(0);

      inAnotherThread(() -> database.transaction(() -> {
        final MutableDocument d = database.lookupByRID(rid[0], true).asDocument().modify();
        d.set("v", 1);
        if (changeTail)
          d.set("s", d.getString("s") + "y");
        d.save();
      }));

      final Document again = database.lookupByRID(rid[0], true).asDocument();
      assertThat(again.getInteger("v")).isEqualTo(0);
      assertThat(again.getString("s")).hasSize(size);
    } finally {
      database.rollback();
    }
  }
}
