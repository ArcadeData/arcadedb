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
 * CHECK DATABASE FIX on a cross-linked chunk chain: the record whose chain runs into a chunk another record already
 * reaches is given its own copy of that shared remainder. Both records keep reading exactly what they read before -
 * which of the two the bytes belonged to is not something the page records - and from then on updating or deleting
 * one of them no longer frees chunks the other one still reads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CrossLinkedChunkRepairTest extends BucketPageLayoutTestSupport {
  private static final String TYPE = "Chunked";

  @Test
  void fixGivesTheSecondRecordItsOwnCopyOfTheSharedChunks() {
    // Long enough for a head plus several continuation chunks, so the shared part is itself a chain
    final RID[] rids = createRecords(150_000);
    crossLink(rids[0], rids[1]);

    final String first = readPayload(rids[0]);
    final String second = readPayload(rids[1]);
    assertThat(second).as("the fabricated cross-link makes the second record read the first one's tail")
        .endsWith(first.substring(first.length() - 1000));

    final Result fixed = checkDatabaseRow(true);
    assertThat(numberProperty(fixed, "crossLinkedChunks")).as(fixed.toJSON().toString()).isEqualTo(1L);
    assertThat(numberProperty(fixed, "crossLinkedChunksRepaired")).as(fixed.toJSON().toString()).isEqualTo(1L);
    assertThat(countRecords(TYPE)).as("a repair deletes nothing").isEqualTo(2L);

    assertThat(readPayload(rids[0])).as("the owner keeps its content").isEqualTo(first);
    assertThat(readPayload(rids[1])).as("the repaired record keeps what it read").isEqualTo(second);

    final Result after = checkDatabaseRow(false);
    assertThat(numberProperty(after, "crossLinkedChunks")).as(after.toJSON().toString()).isZero();
    assertThat(numberProperty(after, "totalErrors")).as(after.toJSON().toString()).isZero();
    assertThat(numberProperty(after, "orphanedChunks")).as(after.toJSON().toString()).isZero();

    // Deleting the original owner frees ITS chunks only
    database.transaction(() -> rids[0].asDocument(true).delete());
    assertThat(readPayload(rids[1])).isEqualTo(second);
    checkDatabase();
  }

  @Test
  void aSharedChunkRightAfterTheHeadIsRepairedToo() {
    final RID[] rids = createRecords(90_000);
    crossLink(rids[0], rids[1]);
    final String second = readPayload(rids[1]);

    final Result fixed = checkDatabaseRow(true);
    assertThat(numberProperty(fixed, "crossLinkedChunksRepaired")).as(fixed.toJSON().toString()).isEqualTo(1L);
    assertThat(readPayload(rids[1])).isEqualTo(second);

    database.transaction(() -> rids[0].asDocument(true).modify().set("payload", "short").save());
    assertThat(readPayload(rids[1])).as("shrinking the owner frees its tail, not the copy").isEqualTo(second);
    checkDatabase();
  }

  @Test
  void aCheckWithoutFixRepairsNothing() {
    final RID[] rids = createRecords(90_000);
    crossLink(rids[0], rids[1]);

    final Result row = checkDatabaseRow(false);
    assertThat(numberProperty(row, "crossLinkedChunks")).isEqualTo(1L);
    assertThat(numberProperty(row, "crossLinkedChunksRepaired")).isZero();
    assertThat(numberProperty(checkDatabaseRow(false), "crossLinkedChunks")).as("still there").isEqualTo(1L);

    // leave the database consistent for the integrity check of the test teardown
    checkDatabaseRow(true);
  }

  private RID[] createRecords(final int size) {
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1).createProperty("payload", Type.STRING);
      rids[0] = database.newDocument(TYPE).set("payload", payload('a', size)).save().getIdentity();
      rids[1] = database.newDocument(TYPE).set("payload", payload('b', size)).save().getIdentity();
    });
    return rids;
  }

  /** Points the second head at the first record's first continuation chunk: head layout is [marker][chunkSize][next]. */
  private void crossLink(final RID owner, final RID other) {
    database.transaction(() -> {
      final long ownerNext = onSlot(owner, page -> page.readLong(recordOffsetOf(page, owner) + 1 + Binary.INT_SERIALIZED_SIZE));
      onSlot(other, page -> {
        page.writeLong(recordOffsetOf(page, other) + 1 + Binary.INT_SERIALIZED_SIZE, ownerNext);
        return 0L;
      });
    });
  }

  private static String payload(final char filler, final int size) {
    final StringBuilder b = new StringBuilder(size);
    for (int i = 0; i < size; i++)
      b.append(i % 97 == 0 ? (char) ('0' + i % 10) : filler);
    return b.toString();
  }

  private String readPayload(final RID rid) {
    final String[] payload = new String[1];
    database.transaction(() -> payload[0] = rid.asDocument(true).getString("payload"));
    return payload[0];
  }
}
