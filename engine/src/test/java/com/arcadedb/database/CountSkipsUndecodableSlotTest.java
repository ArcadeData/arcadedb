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

import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.Test;

import java.io.RandomAccessFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A slot whose size marker cannot even be decoded (garbage where the varint should be) used to make the bucket count
 * throw. The count is what {@code count(*)} answers from once the cached counter is invalidated, and what CHECK DATABASE
 * runs before it reaches the bucket walk that would report and repair the slot - so one garbage slot made both fail. A
 * scan skips such a slot; the count must too, and CHECK DATABASE FIX must get to the slot and remove it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CountSkipsUndecodableSlotTest extends BucketPageLayoutTestSupport {
  private static final String TYPE = "Doc";

  @Test
  void anUndecodableSlotIsSkippedByTheCountAndRemovedByFix() throws Exception {
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1);
      for (int i = 0; i < rids.length; i++)
        rids[i] = database.newDocument(TYPE).set("v", i).save().getIdentity();
    });

    // Eleven bytes with the continuation bit set, written to the file behind the database's back: no varint decoder
    // accepts them, and no commit gets to re-flow the page first
    final long[] offset = new long[1];
    database.transaction(() -> offset[0] = onSlot(rids[1], page -> recordOffsetOf(page, rids[1])));
    final LocalBucket corrupted = bucketOf(TYPE);
    final String filePath = ((DatabaseInternal) database).getFileManager().getFile(corrupted.getFileId()).getFilePath();
    final long pageNumber = rids[1].getPosition() / corrupted.getMaxRecordsInPage();
    final long filePosition = pageNumber * corrupted.getPageSize() + BasePage.PAGE_HEADER_SIZE + offset[0];
    database.close();
    try (final RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
      file.seek(filePosition);
      for (int i = 0; i < 11; i++)
        file.write(0xFF);
    }
    reopenDatabase();

    final LocalBucket bucket = bucketOf(TYPE);
    bucket.setCachedRecordCount(-1);
    final long[] count = new long[1];
    database.transaction(() -> count[0] = bucket.count());
    assertThat(count[0]).as("the undecodable slot is not a record").isEqualTo(2L);

    final Result check = checkDatabaseRow(false);
    assertThat(numberProperty(check, "totalErrors")).as(check.toJSON().toString()).isEqualTo(1L);

    checkDatabaseRow(true);
    final Result after = checkDatabaseRow(false);
    assertThat(numberProperty(after, "totalErrors")).as(after.toJSON().toString()).isZero();
    assertThat(countRecords(TYPE)).isEqualTo(2L);
  }
}
