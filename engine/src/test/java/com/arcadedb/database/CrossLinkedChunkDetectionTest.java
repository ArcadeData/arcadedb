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

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A continuation chunk reached from two record heads (a cross-linked chunk chain) reads as two healthy records until
 * one of them is updated or deleted and frees the chunk under the other. CHECK DATABASE must report it while both
 * chains still parse, and must not "repair" it by deleting either record: the page does not say whose bytes they are.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CrossLinkedChunkDetectionTest extends BucketPageLayoutTestSupport {
  private static final String TYPE    = "Chunked";
  private static final int    PAYLOAD = 90_000;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // the test corrupts the bucket on purpose, and the corruption has no repair
    return false;
  }

  @Test
  void checkDatabaseReportsAChunkSharedByTwoHeads() {
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1).createProperty("payload", Type.STRING);
      rids[0] = database.newDocument(TYPE).set("payload", "a".repeat(PAYLOAD)).save().getIdentity();
      rids[1] = database.newDocument(TYPE).set("payload", "b".repeat(PAYLOAD)).save().getIdentity();
    });
    assertThat((Long) bucketStats(TYPE).get("totalMultiPageRecords")).isEqualTo(2L);
    assertThat(checkDatabaseRow(false).<Long>getProperty("crossLinkedChunks")).isZero();

    // Point the second head at the first record's continuation chunk: head layout is [marker][chunkSize][next].
    database.transaction(() -> {
      final long firstNext = onSlot(rids[0], page -> page.readLong(recordOffsetOf(page, rids[0]) + 1 + Binary.INT_SERIALIZED_SIZE));
      onSlot(rids[1], page -> {
        page.writeLong(recordOffsetOf(page, rids[1]) + 1 + Binary.INT_SERIALIZED_SIZE, firstNext);
        return 0L;
      });
    });

    final Result row = checkDatabaseRow(false);
    assertThat(numberProperty(row, "crossLinkedChunks")).as(row.toJSON().toString()).isEqualTo(1L);
    assertThat(numberProperty(row, "totalErrors")).isGreaterThanOrEqualTo(1L);
    assertThat(warningsOf(row).toString()).contains("cross-linked");

    // FIX reports it again and deletes neither record
    final Result fixed = checkDatabaseRow(true);
    assertThat(numberProperty(fixed, "crossLinkedChunks")).isEqualTo(1L);
    assertThat(countRecords(TYPE)).isEqualTo(2L);
  }
}
