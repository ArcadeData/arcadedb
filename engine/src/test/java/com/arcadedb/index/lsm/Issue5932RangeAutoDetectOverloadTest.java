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
package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Code review follow-up on PR #5961 (issue #5932). {@link LSMTreeIndexMutable#range(Object[], boolean, Object[],
 * boolean)} - the auto-detecting-direction overload, unreachable through the SQL/Cypher engines (every caller goes
 * through the explicit-direction overload via {@link com.arcadedb.index.RangeIndex}) but still a public method on
 * a public class - determined ascending/descending order by comparing the RAW, caller-supplied bounds instead of
 * narrowing them to the index's declared key types first, the exact defect #5932 fixed inside
 * {@link LSMTreeIndexCursor}. A direct caller passing a numeric-string bound against an {@code INTEGER}-typed
 * index would hit the same {@code ClassCastException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5932RangeAutoDetectOverloadTest {

  @Test
  void autoDetectDirectionOverloadAcceptsMismatchedTypeBounds() throws Exception {
    TestHelper.executeInNewDatabase("testIssue5932RangeAutoDetectOverload", db -> {
      db.getSchema().createDocumentType("V").createProperty("n", Type.INTEGER);
      db.command("sql", "CREATE INDEX ON V (n) NOTUNIQUE");
      db.newDocument("V").set("n", 10).save();
      db.newDocument("V").set("n", 20).save();
      db.commit();
      db.begin();

      final TypeIndex typeIndex = db.getSchema().getType("V").getIndexesByProperties("n").get(0);

      for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets()) {
        final LSMTreeIndexMutable mutable = ((LSMTreeIndex) bucketIndex).getMutableIndex();

        assertThatCode(() -> {
          try (final IndexCursor cursor = mutable.range(new Object[] { "5" }, true, new Object[] { "15" }, true)) {
            int count = 0;
            while (cursor.hasNext()) {
              cursor.next();
              ++count;
            }
            assertThat(count).isLessThanOrEqualTo(1); // at most n=10 falls in this bucket
          }
        }).doesNotThrowAnyException();
      }
    });
  }
}
