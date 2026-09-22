/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8040: the record counters persisted in {@code statistics.json} by any build up to and
 * including 26.9.1 may have been double-folded on a Raft replay (#7126). The fix for that (#7162) stops NEW drift,
 * but a counter already wrong on disk is restored verbatim on the next open and never self-corrects, so the release
 * that carries the fix would have carried the wrong numbers forward with it.
 * <p>
 * The file therefore declares a format version. Counts written without one - which is every file written before this
 * change, and the pre-25.2.1 {@code cached-count.json} - are dropped on load, and the bucket recomputes on its next
 * {@code count()}. The marker is written back on the next clean close, so the recount is paid exactly once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8040StatisticsFormatVersionTest extends TestHelper {

  private static final String TYPE    = "Counted";
  private static final int    RECORDS = 25;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE, 1);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("id", i).save();
    });
    // Materialize the counter so there is something for the close to persist.
    assertThat(bucket().count()).isEqualTo(RECORDS);
  }

  @Test
  void aVersionedStatisticsFileKeepsItsCachedCounts() throws IOException {
    reopenDatabase();

    // Written by this build, so the marker is there and the counter is restored without a scan.
    assertThat(statisticsFile().getInt(LocalSchema.STATISTICS_FORMAT_VERSION_KEY, 0))
        .isEqualTo(LocalSchema.STATISTICS_FORMAT_VERSION);
    assertThat(bucket().getCachedRecordCount()).isEqualTo(RECORDS);
  }

  @Test
  void anUnversionedStatisticsFileHasItsCachedCountsDropped() throws IOException {
    database.close();

    // Exactly what a 26.9.1 close leaves behind: the per-bucket entries, no marker. The count is deliberately a
    // wrong one, so a build that trusted the file would answer it and this test would see the drift #8040 reports.
    final JSONObject json = statisticsFile();
    json.remove(LocalSchema.STATISTICS_FORMAT_VERSION_KEY);
    for (final String key : json.keySet())
      json.getJSONObject(key).put("count", RECORDS + 7);
    writeStatisticsFile(json);

    database = factory.open();

    // Not trusted, so not restored ...
    assertThat(bucket().getCachedRecordCount()).isEqualTo(-1L);
    // ... and count(*) answers from the authoritative scan rather than from the wrong number on disk.
    assertThat(bucket().count()).isEqualTo(RECORDS);

    // The recount is paid once: the close below writes the marker, and the next open trusts the file again.
    reopenDatabase();
    assertThat(bucket().getCachedRecordCount()).isEqualTo(RECORDS);
  }

  @Test
  void theLegacyCachedCountFileIsNeverTrusted() throws IOException {
    final String bucketName = bucket().getName();
    database.close();

    final File dir = new File(getDatabasePath());
    Files.delete(new File(dir, LocalSchema.STATISTICS_FILE_NAME).toPath());
    // The <v25.2.1 shape: a flat bucketName -> count map, and older than every build that could have been right.
    try (final FileWriter writer = new FileWriter(new File(dir, LocalSchema.CACHED_COUNT_FILE_NAME_LEGACY))) {
      writer.write(new JSONObject().put(bucketName, RECORDS + 7).toString());
    }

    database = factory.open();

    assertThat(bucket().getCachedRecordCount()).isEqualTo(-1L);
    assertThat(bucket().count()).isEqualTo(RECORDS);
  }

  private LocalBucket bucket() {
    return (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
  }

  private JSONObject statisticsFile() throws IOException {
    final File file = new File(getDatabasePath(), LocalSchema.STATISTICS_FILE_NAME);
    try (final FileInputStream fis = new FileInputStream(file)) {
      return new JSONObject(FileUtils.readStreamAsString(fis, LocalSchema.DEFAULT_ENCODING));
    }
  }

  private void writeStatisticsFile(final JSONObject json) throws IOException {
    try (final FileWriter writer = new FileWriter(new File(getDatabasePath(), LocalSchema.STATISTICS_FILE_NAME))) {
      writer.write(json.toString());
    }
  }
}
