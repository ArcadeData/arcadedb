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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7697: {@code JsonlExporterFormat.exportTimeSeries} chunked its OUTPUT by
 * {@code TIMESERIES_CHUNK_SIZE}, but read every sample through {@code TimeSeriesEngine#iterateQuery}, whose own
 * javadoc says the sealed layer materialises every matching row of the range before the caller sees the first
 * one. An export always asks for {@code Long.MIN_VALUE} to {@code Long.MAX_VALUE} - the widest range there is -
 * so the chunk size bounded the JSON being built while nothing bounded the rows held to build it: exporting a
 * TimeSeries type that does not fit in heap failed before the first chunk was written.
 * <p>
 * The fix walks the samples through {@link com.arcadedb.engine.timeseries.TimeSeriesEngine#forEachRow}, which
 * folds each row into the chunk as it is produced (bounded by one block) instead of collecting them first. This
 * test does not - and given the size that would take, should not - prove the heap bound directly; that is
 * {@code Issue7354BoundedTagScanTest}'s job for {@code forEachRow} itself. What it pins is the observable
 * consequence of switching to a shard-by-shard walk: the export still produces more than one {@code "ts"} chunk
 * once the samples cross the chunk size, and every sample - across every shard, across every chunk - survives
 * the round trip with its value and its timestamp intact, even though the samples are no longer merged into
 * global timestamp order the way {@code iterateQuery} would have merged them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7697JsonlExportStreamsTimeSeriesTest {
  private static final String SOURCE_PATH   = "target/databases/issue7697-jsonl-source";
  private static final String TARGET_PATH   = "target/databases/issue7697-jsonl-target";
  private static final String FILE          = "target/issue7697-jsonl.jsonl.tgz";
  // Comfortably over JsonlExporterFormat's private TIMESERIES_CHUNK_SIZE (1_000), split across 2 shards, so the
  // export must flush more than one "ts" chunk for the type and the walk must cross more than one sealed block.
  private static final int    SAMPLE_COUNT  = 3_500;
  private static final int    SHARDS        = 2;

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File(FILE).delete();
  }

  @Test
  void everySampleAcrossMultipleChunksAndShardsSurvivesTheRoundTrip() throws Exception {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql",
          "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + SHARDS);

      final LocalTimeSeriesType tsType = (LocalTimeSeriesType) source.getSchema().getType("Reading");
      final long[] timestamps = new long[SAMPLE_COUNT];
      final Object[] hosts = new Object[SAMPLE_COUNT];
      final Object[] values = new Object[SAMPLE_COUNT];
      for (int i = 0; i < SAMPLE_COUNT; i++) {
        timestamps[i] = 1_000L + i;
        hosts[i] = "host_" + (i % 4);
        values[i] = (double) i;
      }
      source.begin();
      tsType.getEngine().appendBatch(timestamps, new Object[][] { hosts, values });
      source.commit();
      // Force at least one sealed block per shard, so the walk exercises the sealed layer (not only the mutable
      // bucket) exactly as a large real export would.
      tsType.getEngine().compactAll();
    }

    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();
    assertThat(new File(FILE).exists()).isTrue();

    int tsChunks = 0;
    int samplesInFile = 0;
    for (final String line : readExportedLines()) {
      final JSONObject json = new JSONObject(line);
      if ("ts".equals(json.getString("t"))) {
        tsChunks++;
        samplesInFile += json.getJSONObject("c").getJSONArray("s").length();
      }
    }
    assertThat(tsChunks).as("the export must flush more than one chunk once the samples cross the chunk size")
        .isGreaterThan(1);
    assertThat(samplesInFile).isEqualTo(SAMPLE_COUNT);

    new Importer(
        ("-url " + new File(FILE).getAbsolutePath() + " -database " + TARGET_PATH + " -forceDatabaseCreate true")
            .split(" ")).load();

    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      final LocalTimeSeriesType targetType = (LocalTimeSeriesType) target.getSchema().getType("Reading");
      final List<Object[]> rows = targetType.getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);

      // Correctness, not just count: every (timestamp, value) pair round-trips to the SAME pairing, which a bug
      // that dropped or duplicated a chunk - or mixed up two shards' rows - would not give. query() sorts by
      // timestamp, so this also confirms the import can reconstruct global order from chunks that were no longer
      // written in it.
      assertThat(rows).hasSize(SAMPLE_COUNT);
      for (int i = 0; i < SAMPLE_COUNT; i++) {
        final Object[] row = rows.get(i);
        assertThat((long) row[0]).as("timestamp of row %d", i).isEqualTo(1_000L + i);
        assertThat((double) row[2]).as("value of row %d", i).isEqualTo((double) i);
      }
    }
  }

  private List<String> readExportedLines() throws Exception {
    final List<String> lines = new ArrayList<>();
    try (final BufferedReader reader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(new FileInputStream(FILE)), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null)
        lines.add(line);
    }
    return lines;
  }
}
