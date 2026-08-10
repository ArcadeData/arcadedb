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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression test for #5881: {@code LocalBucket.getRecordPositionInPage()} reported the corrupted-slot RID in its
 * {@code IOException} using {@code int} arithmetic ({@code pageNumber * maxRecordsInPage + positionInPage}), which
 * silently overflows on a bucket beyond ~64GB (page number above ~1_048_576 with the default 2048-slot table) and
 * names the wrong record - exactly the {@code int * int} overflow already fixed for the physical addressing paths
 * by {@link LocalBucket#recordPosition} (#4931). This pins the diagnostic call site to the same widened helper.
 * <p>
 * The corrupted page is a standalone {@link MutablePage} built directly (not persisted through the real bucket
 * file), so a multi-GB bucket is not needed to exercise the overflow boundary.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LocalBucketGetRecordPositionInPageOverflowTest extends TestHelper {

  private static final int OVERFLOW_PAGE_NUMBER = 1_048_576; // first page number past 2^31 with a 2048-slot table
  private static final int POSITION_IN_PAGE     = 5;

  @Test
  void reportsOverflowSafePositionInCorruptionMessage() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.getSchema().createDocumentType("T5881");
    final LocalBucket bucket = (LocalBucket) db.getSchema().getType("T5881").getBuckets(false).get(0);

    final int maxRecordsInPage = bucket.getMaxRecordsInPage();
    // contentHeaderSize and pageSize are protected fields on LocalBucket/PaginatedComponent, readable directly
    // since this test lives in the same package (com.arcadedb.engine).
    final int contentHeaderSize = bucket.contentHeaderSize;

    final MutablePage page = new MutablePage(new PageId(db, bucket.getFileId(), OVERFLOW_PAGE_NUMBER), bucket.pageSize);
    // A nonzero value below contentHeaderSize is what getRecordPositionInPage() treats as a corrupted slot.
    final int corruptSlotValue = contentHeaderSize - 1;
    page.writeUnsignedInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET + POSITION_IN_PAGE * Binary.INT_SERIALIZED_SIZE, corruptSlotValue);

    final long expectedPosition = LocalBucket.recordPosition(OVERFLOW_PAGE_NUMBER, maxRecordsInPage, POSITION_IN_PAGE);
    final long overflowedIntPosition = OVERFLOW_PAGE_NUMBER * maxRecordsInPage + POSITION_IN_PAGE;
    assertThat(overflowedIntPosition).as("the boundary must actually overflow int, or the test proves nothing")
        .isNotEqualTo(expectedPosition);

    try {
      invokeGetRecordPositionInPage(bucket, page, POSITION_IN_PAGE);
      fail("Expected IOException for the corrupted slot");
    } catch (final InvocationTargetException e) {
      assertThat(e.getCause()).isInstanceOf(IOException.class);
      final String message = e.getCause().getMessage();
      assertThat(message).as("diagnostic must report the long, non-overflowed position")
          .endsWith(":" + expectedPosition);
      assertThat(message).as("diagnostic must not report the wrapped int position")
          .doesNotEndWith(":" + overflowedIntPosition);
    }
  }

  private static void invokeGetRecordPositionInPage(final LocalBucket bucket, final BasePage page, final int positionInPage)
      throws Exception {
    final Method m = LocalBucket.class.getDeclaredMethod("getRecordPositionInPage", BasePage.class, int.class);
    m.setAccessible(true);
    m.invoke(bucket, page, positionInPage);
  }
}
