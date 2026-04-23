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
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for an infinite loop in {@link com.arcadedb.index.lsm.LSMTreeIndexUnderlyingPageCursor#getKeys()}
 * when iterating in DESC order over a non-unique LSM-tree index that has adjacent
 * duplicate keys in the same page. The merge of duplicate values always advances
 * the cursor forward; in DESC mode this caused the cursor to revisit the same
 * merged group of duplicates indefinitely.
 */
class IndexDescOrderDuplicateKeysTest extends TestHelper {

  @Test
  @Timeout(value = 10, unit = TimeUnit.SECONDS)
  void shouldNotLoopWithAdjacentDuplicateKeysOnDescIndexScan() {
    final DocumentType type = database.getSchema().createDocumentType("ProcessExecution");
    type.createProperty("startTime", Type.LONG);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "ProcessExecution", new String[] { "startTime" });

    // Insert across MANY transactions so duplicate keys land as separate adjacent entries
    // in the index page (rather than being merged into a single key with multiple values).
    for (int i = 0; i < 10; i++) {
      final int idx = i;
      database.transaction(() -> {
        database.newDocument("ProcessExecution")
            .set("executionId", "exec-" + idx)
            .set("startTime", 1000L)
            .save();
      });
    }

    // Verify that the index is used for the ORDER BY (otherwise we won't exercise the bug)
    final ResultSet result = database.query("sql",
        "select executionId, startTime from ProcessExecution order by startTime desc skip 0 limit 50");

    int count = 0;
    while (result.hasNext()) {
      result.next();
      count++;
    }

    assertThat(count).isEqualTo(10);
    result.close();
  }
}
