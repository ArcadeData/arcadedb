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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for a user-reported scenario where a paginated SELECT backed by an LSM-Tree
 * index returns no rows even though matching records still exist on disk.
 *
 * <p>Scenario:
 * <ul>
 *   <li>Type {@code Entry} is spread across 3 buckets with a non-unique LSM-Tree index on
 *       {@code lvl}.</li>
 *   <li>512 documents are inserted, all with {@code lvl = 1}.</li>
 *   <li>A loop runs {@code SELECT FROM Entry WHERE lvl <= 1 LIMIT 32}, then deletes each
 *       returned record in its own transaction. The loop terminates when the query returns
 *       no rows.</li>
 *   <li>After the loop, {@code countType(Entry, false)} must report 0.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SelectIndexRangeWithDeletesTest extends TestHelper {

  private static final int    N     = 512;
  private static final String ENTRY = "Entry";

  @Test
  void selectWithLimitMustNotTerminateEarlyWhileRecordsStillExist() {
    final Schema schema = database.getSchema();
    final var entryType = schema.createDocumentType(ENTRY);
    entryType.addBucket(schema.createBucket(ENTRY + "_1"));
    entryType.addBucket(schema.createBucket(ENTRY + "_2"));
    entryType.addBucket(schema.createBucket(ENTRY + "_3"));

    entryType.createProperty("msg", Type.STRING);
    entryType.createProperty("lvl", Type.INTEGER);

    entryType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "lvl");

    for (int i = 0; i < N; ++i) {
      database.transaction(() -> {
        final var doc = database.newDocument(ENTRY);
        doc.set("msg", "Hello!");
        doc.set("lvl", 1);
        doc.save();
      });
    }

    assertThat(database.countType(ENTRY, false)).isEqualTo(N);

    int safety = 0;
    while (true) {
      if (++safety > N + 16)
        throw new IllegalStateException("Loop did not terminate after " + safety + " iterations");

      final ResultSet result = database.query("sql",
          "SELECT FROM `" + ENTRY + "` WHERE lvl <= :lvl LIMIT 32", Map.of("lvl", 1));

      if (!result.hasNext())
        break;

      while (result.hasNext()) {
        final Document doc = result.next().toElement().asDocument();
        database.transaction(() -> database.deleteRecord(doc));
      }
    }

    assertThat(database.countType(ENTRY, false))
        .as("all records must be deleted before the query reports an empty result")
        .isZero();
  }
}
