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
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7897, at the entry point that reported it: {@code JsonlExporterFormat.exportTimeSeries} writes a gzip
 * chunk to the archive from inside the {@code TimeSeriesEngine#forEachRow} visitor the #7697 fix gave it, and the
 * visitor used to run under the shard's {@code compactionLock} read lock, so ingest for the type stalled for the
 * whole export.
 * <p>
 * That the lock is no longer held across a visitor is asserted where it can be asserted exactly, on the method
 * this exporter calls: {@code Issue7897ScanDoesNotHoldTheCompactionLockTest} parks a visitor and requires an
 * append to complete anyway. Timing the same thing from out here would only measure how long a 200k-sample export
 * happens to take.
 * <p>
 * What this test is for is the OTHER half of releasing the lock. The export no longer sees one frozen shard, so
 * it has to be shown that a real export, running against a type that is being appended to and compacted
 * throughout, still writes out every sample that existed when it started and writes none of them twice.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("slow")
class Issue7897JsonlExportUnderLiveIngestTest {
  private static final String SOURCE_PATH  = "target/databases/issue7897-jsonl-source";
  private static final String FILE         = "target/issue7897-jsonl.jsonl.tgz";
  private static final int    SAMPLE_COUNT = 50_000;
  /** Bounded, because every compaction rewrites the whole sealed file and an unthrottled loop starves the export. */
  private static final int    WRITER_ROUNDS = 25;
  private static final long   BASE_TS      = 1_700_000_000_000L;
  /** Every timestamp the writer thread appends is at or above this, so the two sets never overlap. */
  private static final long   LIVE_TS_FROM = BASE_TS + (long) SAMPLE_COUNT * 1_000L;

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    new File(FILE).delete();
  }

  @Test
  @Timeout(600)
  void anExportRunningAgainstLiveIngestStillWritesEverySampleExactlyOnce() throws Exception {
    final AtomicBoolean exportRunning = new AtomicBoolean(true);
    final AtomicInteger appendsDuringExport = new AtomicInteger();
    final AtomicInteger compactionsDuringExport = new AtomicInteger();
    final AtomicReference<Throwable> writerFailure = new AtomicReference<>();

    // ONE database instance, exported and written through at the same time - which is what EXPORT DATABASE is: a
    // live-database statement that deliberately does not exclude a writer.
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql",
          "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

      final TimeSeriesEngine engine = ((LocalTimeSeriesType) source.getSchema().getType("Reading")).getEngine();

      final long[] timestamps = new long[SAMPLE_COUNT];
      final Object[] hosts = new Object[SAMPLE_COUNT];
      final Object[] values = new Object[SAMPLE_COUNT];
      for (int i = 0; i < SAMPLE_COUNT; i++) {
        timestamps[i] = BASE_TS + i * 1_000L;
        hosts[i] = "host_" + (i % 4);
        values[i] = (double) i;
      }
      engine.appendBatch(timestamps, new Object[][] { hosts, values });
      // Sealed before the export starts, so all SAMPLE_COUNT of them are in the layer the walk now reads one
      // block at a time - and a compaction landing mid-walk rewrites the very file it is reading.
      engine.compactAll();

      final Thread writer = new Thread(() -> {
        long ts = LIVE_TS_FROM;
        try {
          for (int round = 0; round < WRITER_ROUNDS && exportRunning.get(); round++) {
            engine.appendBatch(new long[] { ts }, new Object[][] { { "host_live" }, { 1.0d } });
            ts += 1_000L;
            appendsDuringExport.incrementAndGet();
            // Seals what was just appended, so the export's walk meets a directory that has been rewritten
            // underneath it rather than one that merely grew.
            engine.compactAll();
            compactionsDuringExport.incrementAndGet();
          }
        } catch (final Throwable t) {
          writerFailure.set(t);
        }
      }, "issue7897-writer");

      try {
        writer.start();
        new Exporter(source, FILE).setFormat("jsonl").setOverwrite(true).exportDatabase();
      } finally {
        exportRunning.set(false);
        writer.join(TimeUnit.SECONDS.toMillis(120));
      }
    }

    assertThat(writerFailure.get()).as("neither the appends nor the compactions may fail against a live export")
        .isNull();
    assertThat(appendsDuringExport.get()).isPositive();
    assertThat(compactionsDuringExport.get()).isPositive();

    final List<Long> exported = exportedTimestamps();

    assertThat(new HashSet<>(exported)).as("no sample may be written out twice").hasSize(exported.size());

    final Set<Long> exportedSet = new HashSet<>(exported);
    final List<Long> missing = new ArrayList<>();
    for (int i = 0; i < SAMPLE_COUNT; i++) {
      final long ts = BASE_TS + i * 1_000L;
      if (!exportedSet.contains(ts))
        missing.add(ts);
    }
    assertThat(missing).as("every sample sealed before the export started must survive it").isEmpty();

    // Nothing invented: below the writer's range there is exactly one sample per original timestamp, which a walk
    // that read a block through an offset belonging to a replaced file would not give.
    final Set<Long> originals = new HashSet<>();
    for (int i = 0; i < SAMPLE_COUNT; i++)
      originals.add(BASE_TS + i * 1_000L);
    for (final Long ts : exported)
      if (ts < LIVE_TS_FROM)
        assertThat(originals).as("exported timestamp %d", ts).contains(ts);
  }

  private List<Long> exportedTimestamps() throws Exception {
    final List<Long> timestamps = new ArrayList<>();
    try (final BufferedReader reader = new BufferedReader(
        new InputStreamReader(new GZIPInputStream(new FileInputStream(FILE)), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        final JSONObject json = new JSONObject(line);
        if (!"ts".equals(json.getString("t")))
          continue;
        final JSONArray samples = json.getJSONObject("c").getJSONArray("s");
        for (int i = 0; i < samples.length(); i++)
          timestamps.add(samples.getJSONArray(i).getLong(0));
      }
    }
    return timestamps;
  }
}
