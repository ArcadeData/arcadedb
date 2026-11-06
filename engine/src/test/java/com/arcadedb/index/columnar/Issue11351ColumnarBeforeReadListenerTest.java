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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.event.BeforeRecordReadListener;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11351: the background passes of a COLUMNAR projection (apply, rebuild) read EXTERNAL values through the database, and must
 * not fire the user's BEFORE READ listeners from there: a veto would diverge the projection from the stored record, and a listener
 * that reads the database takes the database lock a concurrent DROP is waiting behind.
 */
class Issue11351ColumnarBeforeReadListenerTest extends TestHelper {
  private static final long WAIT_MS = 30_000;

  @Test
  void applyDoesNotFireBeforeReadListenersOnTheBackgroundThread() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("Blob", 1);
    type.createProperty("brand", Type.STRING).setExternal(true);
    database.command("sql", "CREATE INDEX ON Blob (brand) COLUMNAR");
    final int bucketId = type.getBuckets(false).getFirst().getFileId();

    final Thread testThread = Thread.currentThread();
    final AtomicInteger backgroundCalls = new AtomicInteger();
    // vetoes every read made outside the test thread
    final BeforeRecordReadListener listener = rid -> {
      if (Thread.currentThread() == testThread)
        return true;
      backgroundCalls.incrementAndGet();
      return false;
    };
    database.getEvents().registerListener(listener);
    try {
      final String value = "x".repeat(300);
      database.transaction(() -> database.newDocument("Blob").set("brand", value).save());

      assertThat(manager().awaitCaughtUp(WAIT_MS)).isTrue();
      assertThat(backgroundCalls.get()).as("no listener fired on a background thread").isZero();
      assertThat(brandCounts(manager().projection(bucketId))).as("the projection keeps the stored value")
          .isEqualTo(Map.of(value, 1L));
    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }

  @Test
  void rebuildDoesNotFireBeforeReadListenersOnTheBackgroundThread() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("Blob", 1);
    type.createProperty("brand", Type.STRING).setExternal(true);
    final String value = "y".repeat(300);
    database.transaction(() -> database.newDocument("Blob").set("brand", value).save());
    final int bucketId = type.getBuckets(false).getFirst().getFileId();

    final Thread testThread = Thread.currentThread();
    final AtomicInteger backgroundCalls = new AtomicInteger();
    final BeforeRecordReadListener listener = rid -> {
      if (Thread.currentThread() == testThread)
        return true;
      backgroundCalls.incrementAndGet();
      return false;
    };
    database.getEvents().registerListener(listener);
    try {
      database.command("sql", "CREATE INDEX ON Blob (brand) COLUMNAR");
      assertThat(manager().awaitCaughtUp(WAIT_MS)).isTrue();
      assertThat(backgroundCalls.get()).as("no listener fired on a background thread").isZero();
      assertThat(brandCounts(manager().projection(bucketId))).isEqualTo(Map.of(value, 1L));
    } finally {
      database.getEvents().unregisterListener(listener);
    }
  }

  private static Map<Object, Long> brandCounts(final ColumnarProjection projection) {
    final Map<Object, Long> counts = new TreeMap<>();
    try (final ColumnarSnapshot snapshot = projection.acquire()) {
      final GlobalDictionary dictionary = snapshot.dictionary("brand");
      for (final ColumnarSegmentView view : snapshot.segments()) {
        final PositionBitmap all = new PositionBitmap(projection.maxRecordsInPage());
        for (final int page : view.live().pages())
          all.setPage(page);
        final ColumnReader reader = view.column("brand");
        if (reader != null)
          reader.forEachOrdinal(all, (position, ordinal) -> counts.merge(dictionary.valueOf(ordinal), 1L, Long::sum));
      }
    }
    return counts;
  }

  private ColumnarIndexManager manager() {
    return ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager();
  }
}
