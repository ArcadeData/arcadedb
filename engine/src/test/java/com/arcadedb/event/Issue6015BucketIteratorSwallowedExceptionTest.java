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
package com.arcadedb.event;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #6015.
 * <p>
 * {@code BucketIterator.fetchNext()} used to catch every {@code Exception} raised while materializing a record -
 * including an arbitrary bug in a user-supplied {@link AfterRecordReadListener} - log it at {@code SEVERE}, and
 * silently drop the record from the result. The caller could not tell "this bucket legitimately has fewer
 * records" from "a record failed to load and was skipped": this is exactly what turned #5976's real
 * {@code IllegalArgumentException} into a misleading {@code NoSuchElementException} several layers up.
 * <p>
 * The fix narrows the catch to {@link com.arcadedb.exception.RecordNotFoundException} (a benign concurrent-delete
 * race) and {@link com.arcadedb.exception.SerializationException} (a known-corrupt on-disk record, still logged
 * and skipped so one bad record does not abort an otherwise healthy full scan). Every other exception - such as
 * this test's simulated listener bug - now propagates out of the iterator instead of being swallowed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6015BucketIteratorSwallowedExceptionTest extends TestHelper implements AfterRecordReadListener {

  private static final String TYPE_NAME = "Issue6015Type";

  @Test
  void listenerBugDuringScanPropagatesInsteadOfLookingLikeAnEmptyBucket() {
    final VertexType type = database.getSchema().createVertexType(TYPE_NAME);
    type.getEvents().registerListener((AfterRecordReadListener) this);

    database.transaction(() -> database.newVertex(TYPE_NAME).set("id", 1).save());

    // REPEATABLE_READ forces content (and therefore AfterRecordReadListener) to be loaded eagerly by
    // lookupByRID() inside BucketIterator.fetchNext(), the same shape #5976 needed to surface the bug.
    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThatThrownBy(() -> database.transaction(() -> database.iterateType(TYPE_NAME, true).next()))
          .as("a real bug in a read listener must surface, not be silently swallowed as \"the bucket is empty\"")
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("simulated listener bug");
    } finally {
      database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    }
  }

  @Override
  public Record onAfterRead(final Record record) {
    throw new IllegalStateException("simulated listener bug (#6015)");
  }
}
