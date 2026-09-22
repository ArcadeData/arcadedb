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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.exporter.format.JsonlExporterFormat;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8166, the observability half: an {@code EXPORT DATABASE} that lost sealed blocks mid-walk wrote a SHORT
 * file and said nothing about it.
 * <p>
 * The engine has counted those blocks since #8043 - {@code AggregationMetrics.addVanishedBlock} - but the only
 * reader of that count anywhere in the tree was the PromQL/HTTP metrics surface, and
 * {@code JsonlExporterFormat.exportTimeSeries} passed {@code null} for the metrics. So the one caller the
 * counter was added for could not see it: no exception, no log line, no count. The export now passes an
 * {@code AggregationMetrics}, logs a WARNING naming the type, and reports the total under
 * {@code vanishedTimeSeriesBlocks} in the export's own statistics.
 * <p>
 * Reported, not fatal, and that is deliberate: retention dropping blocks older than the policy while a long
 * export runs is legitimate, and those samples are genuinely gone rather than somewhere else - unlike a block a
 * DOWNSAMPLE coarsened, for which the engine raises instead (see {@code Issue8166WalkAcrossDownsampleTest}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8166">issue #8166</a>
 */
class Issue8166ExportCountsVanishedBlocksTest {
  private static final String SOURCE_PATH = "target/databases/issue8166-export-source";
  private static final String FILE        = "target/issue8166-export.jsonl.tgz";
  private static final long   BASE_TS     = 1_700_000_000_000L;
  /**
   * Over two sealed blocks: {@code DeltaOfDeltaCodec.MAX_BLOCK_SIZE} is 65,536 samples, so this compacts into
   * three, which is the minimum that lets a truncate strand blocks a walk has not reached yet.
   */
  private static final int    SAMPLES     = 140_000;

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    new File(FILE).delete();
  }

  @Test
  void anExportThatLosesBlocksToRetentionReportsThemInsteadOfWritingSilentlyShort() throws Exception {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql",
          "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

      final TimeSeriesEngine engine = fill("Reading", source, SAMPLES);

      assertThat(engine.getShard(0).getSealedStore().getBlockCount())
          .as("the walk needs blocks it has not reached yet for a truncate to strand any").isGreaterThan(2);

      final ExporterContext context = new ExporterContext();
      final ExporterSettings settings = new ExporterSettings();
      settings.file = FILE;
      settings.format = JsonlExporterFormat.NAME;
      settings.overwriteFile = true;

      // The retention pass, fired from inside the export's own chunk flush so it lands mid-walk deterministically
      // rather than by racing a background thread against it.
      final JsonlExporterFormat format = new JsonlExporterFormat((DatabaseInternal) source, settings, context,
          new ConsoleLogger(0)) {
        private boolean fired;

        @Override
        protected void writeJsonLine(final String type, final JSONObject json) throws IOException {
          super.writeJsonLine(type, json);
          if (!fired && "ts".equals(type)) {
            fired = true;
            // Keeps only the last block; every block after the one the walk is inside is now stranded.
            engine.getShard(0).getSealedStore().truncateBefore(BASE_TS + (SAMPLES - 1_000L) * 1_000L);
          }
        }
      };

      format.exportDatabase();

      assertThat(context.vanishedTimeSeriesBlocks.get())
          .as("the blocks retention removed from under the walk must be counted, not stepped over in silence")
          .isGreaterThan(0);
      assertThat(context.timeSeriesSamples.get())
          .as("and the export is short, which is the whole reason the count has to exist")
          .isLessThan(SAMPLES);
    }
  }

  /**
   * The other half of the review point on PR #8197: a downsample landing mid-export fails THAT TYPE, not the
   * whole export.
   * <p>
   * The engine raises for a coarsened block rather than answering short, and an unchecked exception propagating
   * out of {@code exportTimeSeries} would abort a run that may already have written every vertex, edge, document
   * and other TIMESERIES type - making an operator re-run hours of work to learn about one series. It is counted
   * as a skipped record instead, which is the mechanism issue #6471 established for a part of an export that
   * could not be written: every other type is still exported, and {@code Exporter} turns a non-zero count into a
   * failed outcome at the end, so the run is loudly incomplete rather than silently short.
   */
  @Test
  void aDownsampleMidExportFailsOnlyItsOwnTypeAndTheRestIsStillWritten() throws Exception {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql",
          "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
      source.command("sql",
          "CREATE TIMESERIES TYPE Other TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

      final TimeSeriesEngine big = fill("Reading", source, SAMPLES);
      fill("Other", source, 2_000);

      assertThat(big.getShard(0).getSealedStore().getBlockCount())
          .as("the walk needs a block it has not reached for the downsample to strand one").isGreaterThan(2);

      final ExporterContext context = new ExporterContext();
      final ExporterSettings settings = new ExporterSettings();
      settings.file = FILE;
      settings.format = JsonlExporterFormat.NAME;
      settings.overwriteFile = true;

      final JsonlExporterFormat format = new JsonlExporterFormat((DatabaseInternal) source, settings, context,
          new ConsoleLogger(0)) {
        private boolean fired;

        @Override
        protected void writeJsonLine(final String type, final JSONObject json) throws IOException {
          super.writeJsonLine(type, json);
          // Fired from inside the export's own chunk flush, on whichever type is being written, so the rewrite
          // lands mid-walk deterministically instead of by racing a background thread against it.
          if (!fired && "ts".equals(type) && "Reading".equals(json.getString("t"))) {
            fired = true;
            try {
              big.getShard(0).getSealedStore()
                  .downsampleBlocks(Long.MAX_VALUE, 60_000L, 0, List.of(1), List.of(2));
            } catch (final IOException e) {
              throw new IllegalStateException("the downsample itself must not be what failed", e);
            }
          }
        }
      };

      format.exportDatabase();

      assertThat(context.skippedRecords.get())
          .as("the coarsened type is recorded as a gap, which makes the export a failed outcome").isEqualTo(1);
      assertThat(context.partialTimeSeriesTypes.get())
          .as("and named separately, because unlike a skipped record it DID leave rows in the archive")
          .isEqualTo(1);
      assertThat(tsTypesInExport())
          .as("which is the point of the distinction: there really are rows on disk under its name")
          .contains("Reading");

      // THE invariant, and the one a `continue` past the trailing flush broke: every row the visitor counted is
      // a row in the file. The rows buffered since the last chunk boundary - up to TIMESERIES_CHUNK_SIZE - 1 of
      // them - are already counted in timeSeriesSamples when the refusal fires, so dropping them leaves the
      // summary claiming samples the archive does not hold (review of PR #8197). Asserted across both types,
      // because that is the number an operator reads.
      assertThat(tsSamplesInExport())
          .as("a sample counted but not written is exactly the silently-short answer this PR is about")
          .isEqualTo(context.timeSeriesSamples.get());
      assertThat(context.timeSeriesSamples.get())
          .as("and the OTHER type was still exported rather than lost with it").isGreaterThan(0);

      final List<String> types = tsTypesInExport();
      assertThat(types).as("the export went on past the type it could not finish").contains("Other");
    }
  }

  /**
   * The control: an export nobody disturbs reports no vanished block and writes every sample, so passing the
   * metrics through did not turn an ordinary export into a suspicious one.
   */
  @Test
  void anUndisturbedExportReportsNoVanishedBlock() throws Exception {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql",
          "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");

      fill("Reading", source, 2_000);

      final Map<String, Object> result = new Exporter(source, FILE).setFormat(JsonlExporterFormat.NAME)
          .setOverwrite(true).exportDatabase();

      assertThat(result).doesNotContainKey("vanishedTimeSeriesBlocks");
      assertThat(result).containsEntry("timeSeriesSamples", 2_000L);
    }
  }

  // ---- Helpers ----

  /** Appends {@code samples} rows to an existing TIMESERIES type and seals them, returning its engine. */
  private static TimeSeriesEngine fill(final String typeName, final Database source, final int samples)
      throws IOException {
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) source.getSchema().getType(typeName)).getEngine();
    final long[] timestamps = new long[samples];
    final Object[] hosts = new Object[samples];
    final Object[] values = new Object[samples];
    for (int i = 0; i < samples; i++) {
      timestamps[i] = BASE_TS + i * 1_000L;
      hosts[i] = "host_" + (i % 4);
      values[i] = (double) i;
    }
    source.begin();
    engine.appendBatch(timestamps, new Object[][] { hosts, values });
    source.commit();
    engine.compactAll();
    return engine;
  }

  /** Every TIMESERIES sample actually present in the written archive, across all types. */
  private static long tsSamplesInExport() throws IOException {
    long samples = 0;
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(
        new GZIPInputStream(new FileInputStream(FILE)), StandardCharsets.UTF_8))) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        if (line.isBlank())
          continue;
        final JSONObject json = new JSONObject(line);
        if ("ts".equals(json.getString("t")))
          samples += json.getJSONObject("c").getJSONArray("s").length();
      }
    }
    return samples;
  }

  /** The TIMESERIES type names that actually have a {@code "ts"} line in the written archive. */
  private static List<String> tsTypesInExport() throws IOException {
    final List<String> types = new ArrayList<>();
    try (final BufferedReader reader = new BufferedReader(new InputStreamReader(
        new GZIPInputStream(new FileInputStream(FILE)), StandardCharsets.UTF_8))) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        if (line.isBlank())
          continue;
        final JSONObject json = new JSONObject(line);
        if ("ts".equals(json.getString("t"))) {
          final String name = json.getJSONObject("c").getString("t");
          if (!types.contains(name))
            types.add(name);
        }
      }
    }
    return types;
  }
}
