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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.Bucket;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * {@code removeBucketInternal} shrank the bucket list but never re-bound the type's bucket-selection strategy, so the
 * strategy's cached {@code total} stayed at the pre-removal bucket count. Round-robin selection keeps handing out
 * indexes up to the stale {@code total}, and once that reaches the old last index the type's own {@code buckets.get(...)}
 * throws {@link IndexOutOfBoundsException} because the bucket at that index is gone.
 * <p>
 * {@code addBucketInternal} already rebinds the strategy after growing the list (symmetric fix, issue #6380).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6380RemoveBucketStrategyRefreshTest extends TestHelper {

  @Test
  void insertAfterRemovingBucketDoesNotThrowIndexOutOfBounds() {
    database.transaction(() -> database.getSchema().createDocumentType("Product", 3));

    // Detach one of the three buckets, leaving a still-populated (well, still-usable) 2-bucket type behind.
    database.transaction(() -> {
      final DocumentType type = database.getSchema().getType("Product");
      final Bucket lastBucket = type.getBuckets(false).get(type.getBuckets(false).size() - 1);
      type.removeBucket(lastBucket);
    });

    // Round-robin's cursor starts at -1: three inserts push it through indexes 0, 1 and then the stale index 2, which
    // is exactly the index that used to be valid before the bucket was removed. Without the fix, the third insert
    // throws IndexOutOfBoundsException instead of wrapping back to index 0 against the now-correct 2-bucket total.
    assertThatCode(() -> database.transaction(() -> {
      for (int i = 0; i < 6; i++) {
        final MutableDocument doc = database.newDocument("Product");
        doc.set("id", i);
        doc.save();
      }
    })).doesNotThrowAnyException();
  }
}
